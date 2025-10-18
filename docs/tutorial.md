# Tutorial: a durable subscription renewal

This tutorial builds a small but realistic workflow from scratch: renewing a customer's
subscription every month, retrying failed payments, giving the customer a grace period
to update their card, and letting support pause a subscription. Along the way it covers
steps, durable timers, signals, queries, testing, running a worker, and operating runs
from the command line.

You need Python 3.12 and `pip install "loomwork[sqlite]"`.

## 1. Steps: the parts that touch the outside world

Anything with side effects (a payment API, an email, a database write) is a **step**.
Steps run on workers, are retried according to their policy, and their results are
recorded so they never run again when the workflow replays.

```python
# billing/steps.py
from datetime import timedelta

import loomwork as lw

from billing import gateway, mailer


@lw.step(
    name="billing.charge",
    retry=lw.RetryPolicy(initial_interval=30, backoff=2.0, max_attempts=4),
    timeout=timedelta(seconds=20),
)
async def charge(customer_id: str, cents: int, idempotency_key: str) -> str:
    response = await gateway.charge(customer_id, cents, idempotency_key=idempotency_key)
    if response.declined:
        # A decline will not succeed on retry: fail the step immediately.
        raise lw.ApplicationError(response.reason, type="CardDeclined", non_retryable=True)
    return response.charge_id


@lw.step(name="billing.email")
async def email(customer_id: str, template: str) -> None:
    await mailer.send(customer_id, template)
```

Two details matter. The retry policy handles transient failures (timeouts, 5xx
responses) with backoff; a declined card is a business outcome, so it is raised as a
non-retryable `ApplicationError`. And the payment call takes an idempotency key: a step
can run more than once if a worker dies after charging but before recording the result,
so the payment provider must be able to recognise the repeat.

## 2. The workflow: plain async Python

```python
# billing/workflows.py
from datetime import timedelta

import loomwork as lw

from billing.steps import charge, email


@lw.workflow(name="billing.subscription")
class Subscription:
    def __init__(self, customer_id: str, cents: int) -> None:
        self.paused = False
        self.has_new_card = False
        self.renewals = 0

    @lw.run
    async def run(self, customer_id: str, cents: int) -> str:
        while True:
            await lw.sleep(timedelta(days=30))
            await lw.wait_condition(lambda: not self.paused)

            key = f"{lw.info().workflow_id}/{self.renewals}"
            try:
                await charge(customer_id, cents, key)
            except lw.StepError:
                if not await self.grace_period(customer_id):
                    await email(customer_id, "subscription-cancelled")
                    return "cancelled"
                await charge(customer_id, cents, key + "/retry")
            self.renewals += 1
            await email(customer_id, "receipt")
            if lw.continue_as_new_suggested():
                lw.continue_as_new(customer_id, cents)

    async def grace_period(self, customer_id: str) -> bool:
        await email(customer_id, "payment-failed")
        self.has_new_card = False
        try:
            await lw.wait_condition(lambda: self.has_new_card, timeout=timedelta(days=7))
        except TimeoutError:
            return False
        return True

    @lw.signal
    def card_updated(self, _: bool) -> None:
        self.has_new_card = True

    @lw.signal
    def set_paused(self, paused: bool) -> None:
        self.paused = paused

    @lw.query
    def status(self) -> dict[str, object]:
        return {"paused": self.paused, "renewals": self.renewals}
```

Read it as if it were a script that runs for years, because that is what it is:

- `lw.sleep(timedelta(days=30))` is a **durable timer**. No process waits for 30 days;
  the run is stored, and a worker picks it up again when the timer fires, after any
  number of restarts and deploys.
- `wait_condition` blocks until a signal changes the state: support pausing the
  subscription, or the customer updating their card within the seven-day grace period.
- The idempotency key is built from the workflow id and the renewal count, which are the
  same on every replay; `uuid.uuid4()` would not be (use `lw.uuid4()` for random ids).
- A subscription that renews every month for years would build up a long history.
  `continue_as_new` starts a fresh run of the same workflow id with the current
  arguments, and the history starts over.

Workflow code must be **deterministic**: given the same history it must make the same
decisions. That rules out reading the clock (`lw.now()` instead), random numbers
(`lw.random()`), environment variables and network calls (those go in steps). The linter
catches most mistakes:

```bash
loomwork lint billing/
```

## 3. Test it, including the waiting

The testkit runs workflows in memory and skips idle time, so the month-long sleeps and
the week-long grace period take milliseconds:

```python
# tests/test_subscription.py
import loomwork as lw
from loomwork.testing import WorkflowEnvironment

from billing.steps import charge, email
from billing.workflows import Subscription


async def test_declined_card_without_update_cancels() -> None:
    async with WorkflowEnvironment(workflows=[Subscription], steps=[charge, email]) as env:
        env.mock_step(
            charge, side_effect=lw.ApplicationError("declined", type="CardDeclined", non_retryable=True)
        )
        emails = env.mock_step(email, return_value=None)

        result = await env.execute(Subscription, "cus_1", 999)

        assert result == "cancelled"
        templates = [call.args[1] for call in emails.calls]
        assert templates == ["payment-failed", "subscription-cancelled"]
```

See [testing](testing.md) for signalling a run mid-test, golden histories that catch
incompatible changes, and the pytest plugin.

## 4. Run it

An application process starts runs; workers execute them. Both point at the same store.
For a single machine, SQLite is enough:

```bash
export LOOMWORK_STORE_URL=sqlite:///billing.db

# Terminal 1: a worker for the billing workflows.
loomwork worker --app billing.workflows --metrics-port 9464
```

`--app billing.workflows` collects every workflow and step visible at the top level of
that module, including the steps it imports from `billing.steps`. (To use a specific
`Loomwork` app instead, with its payload codecs, point at it: `--app billing.app:app`.)
Stop the worker with Ctrl-C: in-flight steps get a grace period to finish.

Keep attribute names and handler method names apart (`self.has_new_card` and the
`card_updated` signal above): an instance attribute with a handler's name would hide the
method.

```bash
# Terminal 2: start a subscription and look at it.
loomwork workflow start billing.subscription '"cus_1"' 999 --id sub-cus_1
loomwork workflow describe sub-cus_1
loomwork workflow query sub-cus_1 status --app billing.workflows
loomwork workflow signal sub-cus_1 set_paused true
loomwork workflow history sub-cus_1
```

From Python, the same through a client:

```python
from loomwork.store import store_from_url

async with lw.Loomwork(store=store_from_url("sqlite:///billing.db"), run_worker=False) as app:
    handle = await app.start_workflow(Subscription, "cus_1", 999, id="sub-cus_1")
    await handle.signal(Subscription.set_paused, True)
    print(await handle.query(Subscription.status))
```

## 5. Change it safely

Suppose you now want to send a reminder three days before each renewal. Runs that are
already sleeping recorded a single 30-day timer; new code that sleeps 27 days and then 3
would not match their histories. Guard the change:

```python
if lw.patched("renewal-reminder"):
    await lw.sleep(timedelta(days=27))
    await email(customer_id, "renewal-reminder")
    await lw.sleep(timedelta(days=3))
else:
    await lw.sleep(timedelta(days=30))
```

Runs that recorded the old path keep taking it; new runs, and runs that reach this point
after the deploy, take the new one. Before deploying, check the new code against what is
in production:

```bash
loomwork compat --app billing.workflows       # exits 1 if any run would break
```

The [versioning guide](guides/versioning.md) covers retiring a patch once old runs are
gone.

## 6. Go to production

- Use PostgreSQL (`pip install "loomwork[postgres]"`, `postgresql://` store URL) and run
  as many workers as you need; they coordinate through the database.
- Or run `loomwork server` in front of the database and give workers a token instead of
  database credentials ([server mode](server.md)).
- Encrypt payloads so card data never reaches the database in the clear
  ([payload encryption](payload-encryption.md)).
- Scrape the workers' `/metrics` and alert on failing ticks and growing queue delay
  ([monitoring](operations/monitoring.md)).
- [`deploy/`](../deploy/README.md) has a container image, a Compose stack and Kubernetes
  manifests to start from.
