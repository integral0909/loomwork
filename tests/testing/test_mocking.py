"""``WorkflowEnvironment.mock_step``: replacing a step's implementation for one
environment, without touching the ``StepDefinition`` object itself."""

from __future__ import annotations

import pytest

import loomwork as lw
from loomwork import RetryPolicy, StepError
from loomwork.testing import WorkflowEnvironment

pytestmark = pytest.mark.integration

FAST_RETRY = RetryPolicy(initial_interval=0.001, max_interval=0.01, max_attempts=3, jitter=0)


@lw.step(name="mock.charge_card", retry=FAST_RETRY)
async def charge_card(order_id: str, amount_cents: int) -> str:
    raise AssertionError("the real implementation must never run once mocked")


@lw.step(name="mock.ship")
async def ship(order_id: str) -> str:
    return f"shipped:{order_id}"


@lw.workflow(name="mock.process_order")
async def process_order(order_id: str, amount_cents: int) -> dict[str, str]:
    charge_id = await charge_card(order_id, amount_cents)
    tracking = await ship(order_id)
    return {"charge_id": charge_id, "tracking": tracking}


@lw.workflow(name="mock.call_twice")
async def call_twice(order_id: str) -> list[str]:
    first = await charge_card(order_id, 100)
    second = await charge_card(order_id, 200)
    return [first, second]


async def test_return_value_replaces_the_real_implementation() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        env.mock_step(charge_card, return_value="ch_mocked")
        result = await env.execute(process_order, "order-1", 5_000)

    assert result == {"charge_id": "ch_mocked", "tracking": "shipped:order-1"}


async def test_unmocked_steps_still_run_for_real() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        mock = env.mock_step(charge_card, return_value="ch_1")
        result = await env.execute(process_order, "order-2", 1_000)

    assert result["tracking"] == "shipped:order-2"  # ship() ran unmocked
    mock.assert_called_once()
    mock.assert_called_with("order-2", 1_000)


async def test_mock_records_every_call() -> None:
    async with WorkflowEnvironment(workflows=[call_twice], steps=[charge_card]) as env:
        mock = env.mock_step(charge_card, return_value="ch")
        await env.execute(call_twice, "order-3")

    assert mock.call_count == 2
    assert [call.args for call in mock.calls] == [("order-3", 100), ("order-3", 200)]
    mock.assert_any_call("order-3", 100)
    mock.assert_any_call("order-3", 200)


async def test_side_effect_exception_triggers_the_real_retry_policy() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        mock = env.mock_step(charge_card, side_effect=ConnectionError("gateway down"))
        with pytest.raises(lw.WorkflowFailedError) as excinfo:
            await env.execute(process_order, "order-4", 1_000)

    error = excinfo.value.failure.to_exception()
    assert isinstance(error, StepError)
    assert error.attempts == FAST_RETRY.max_attempts
    # Every attempt (not just the last) actually invoked the mock.
    assert mock.call_count == FAST_RETRY.max_attempts


async def test_side_effect_callable_computes_the_result() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        env.mock_step(charge_card, side_effect=lambda order_id, amount: f"ch_{order_id}_{amount}")
        result = await env.execute(process_order, "order-5", 750)

    assert result["charge_id"] == "ch_order-5_750"


async def test_side_effect_list_gives_a_different_outcome_per_call() -> None:
    async with WorkflowEnvironment(workflows=[call_twice], steps=[charge_card]) as env:
        env.mock_step(charge_card, side_effect=[ValueError("first call fails"), "ch_ok"])
        with pytest.raises(lw.WorkflowFailedError):
            # The first call exhausts its own retries (each attempt re-consumes the
            # *same* list entry) and fails the run before a second call ever happens.
            await env.execute(call_twice, "order-6")


async def test_side_effect_list_across_two_independent_calls() -> None:
    async with WorkflowEnvironment(workflows=[call_twice], steps=[charge_card]) as env:
        mock = env.mock_step(charge_card, side_effect=["ch_first", "ch_second"])
        result = await env.execute(call_twice, "order-7")

    assert result == ["ch_first", "ch_second"]
    assert mock.call_count == 2


async def test_mock_step_accepts_a_plain_name() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        env.mock_step("mock.charge_card", return_value="ch_by_name")
        result = await env.execute(process_order, "order-8", 1_000)

    assert result["charge_id"] == "ch_by_name"


async def test_mock_does_not_mutate_the_shared_step_definition() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        env.mock_step(charge_card, return_value="ch_env_only")
        await env.execute(process_order, "order-9", 1_000)

    # The original StepDefinition passed in is untouched: another environment (or
    # production code importing the same module) still runs the real function.
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as other:
        with pytest.raises(lw.WorkflowFailedError):
            await other.execute(process_order, "order-10", 1_000)


async def test_unmock_step_restores_the_real_implementation() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        env.mock_step(charge_card, return_value="ch_mocked")
        env.unmock_step(charge_card)
        with pytest.raises(lw.WorkflowFailedError):
            await env.execute(process_order, "order-11", 1_000)


async def test_unmock_step_accepts_a_plain_name() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        env.mock_step(charge_card, return_value="ch_mocked")
        env.unmock_step("mock.charge_card")
        with pytest.raises(lw.WorkflowFailedError):
            await env.execute(process_order, "order-12", 1_000)


async def test_unmock_step_that_was_never_mocked_raises() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        with pytest.raises(ValueError, match="not currently mocked"):
            env.unmock_step(charge_card)


async def test_mocking_again_replaces_the_previous_mock() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        first = env.mock_step(charge_card, return_value="ch_first")
        second = env.mock_step(charge_card, return_value="ch_second")
        result = await env.execute(process_order, "order-13", 1_000)

    assert result["charge_id"] == "ch_second"
    first.assert_not_called()
    second.assert_called_once()
    # unmock_step still restores the *original* real implementation, not the first mock.
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env2:
        env2.mock_step(charge_card, return_value="ch")
        env2.mock_step(charge_card, return_value="ch2")
        env2.unmock_step(charge_card)
        with pytest.raises(lw.WorkflowFailedError):
            await env2.execute(process_order, "order-14", 1_000)


async def test_assert_not_called_reports_how_many_times_it_was_called() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        mock = env.mock_step(charge_card, return_value="ch")
        await env.execute(process_order, "order-15", 1_000)

    with pytest.raises(AssertionError, match="was called 1 time"):
        mock.assert_not_called()


async def test_assert_called_with_reports_the_actual_arguments() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        mock = env.mock_step(charge_card, return_value="ch")
        await env.execute(process_order, "order-16", 1_000)

    with pytest.raises(AssertionError, match="expected"):
        mock.assert_called_with("order-16", 999)


async def test_assert_called_on_a_mock_with_no_calls_raises() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card, ship]) as env:
        mock = env.mock_step(charge_card, return_value="ch")

        with pytest.raises(AssertionError, match="never called"):
            mock.assert_called()
        with pytest.raises(AssertionError, match="never called"):
            mock.assert_called_with("anything")
