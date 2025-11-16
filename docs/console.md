# Operator web console

A server-rendered web UI for operating workflows in server mode: browsing and filtering
runs, inspecting one run's full history and pending work, sending it a signal, cancelling
or terminating it, and viewing and controlling schedules. It lives at `/console`,
alongside the worker and control APIs (see [server mode](server.md)).

The console is a plain multi-page app: every page is rendered on the server and every
action is an ordinary HTML form POST with a redirect back to a GET. There is no
JavaScript, no client-side framework, and no build step -- the one static asset is a
hand-written stylesheet.

## Enabling it

The console is mounted whenever `create_app` builds the server, controlled by
`[server.console]`:

```toml
[server.console]
enabled = true                  # default
# session_secret = "..."        # see "Sessions" below
session_ttl_seconds = 28800     # 8 hours, default
```

Set `enabled = false` to remove it entirely (`/console/*` then 404s; the rest of the
server is unaffected).

### Configuration reference

| Field | Default | Description |
| ----- | ------- | ----------- |
| `enabled` | `true` | Mount the console at `/console`. |
| `session_secret` | none | HMAC key that signs session cookies. Unset generates a random key per process (sessions do not survive a restart; fine for a single long-running server, set this explicitly if you restart often or run more than one server process behind a load balancer sharing the same key). |
| `session_ttl_seconds` | `28800` (8h) | How long a signed-in session stays valid. |
| `search_attributes` | `{}` | Display-only override: `{name: type}` shown next to the runs-list and batch query boxes, in place of the real schema, when set. Left unset (the default), the console shows and enforces the same schema: `[server.search_attributes]` (see [server mode](server.md)), which is what actually controls which attributes a query may reference. |

## Authentication and the session model

Console auth reuses the server's existing bearer tokens (`loomwork.server.auth`) --
there is no separate console user store. Signing in pastes the same token a worker or
`curl` would use in an `Authorization: Bearer` header:

1. `POST /console/login` sends the token as a form field. It is checked the same way any
   API request checks a bearer token (`TokenFile.authenticate`); a valid token gets a
   session, an invalid one sees an error without ever touching a cookie.
2. The session is a signed, `HttpOnly`, `SameSite=Strict` cookie (`Secure` too, once the
   request arrives over HTTPS) holding the token's id, a random CSRF secret, and an
   expiry, HMAC-signed with `session_secret` (or the per-process random key). The
   *token's secret itself never appears in the cookie* -- only its id -- so every
   request re-resolves the id against the live token file
   (`ServerState.current_tokens()`), the same call the JSON APIs make. Revoking or
   rotating a token therefore invalidates every console session using it within about a
   second, exactly like it does for API callers, even though the browser's session
   cookie has not expired.
3. Every page and action re-checks the token's roles and namespaces with
   `loomwork.server.auth.require` -- the identical function the control and worker APIs
   call. Viewing runs and schedules needs `control_read` (granted by `reader`,
   `operator` and `admin`); sending a signal, cancelling, terminating, or pausing/
   unpausing/triggering a schedule needs `control_write` (`operator` and `admin`). A
   token scoped to specific namespaces only sees and can act on those namespaces, and
   gets the same `403` for any other whether or not it is configured -- it cannot learn
   what other namespaces exist, matching the JSON APIs exactly.
4. `POST /console/logout` clears the cookie.
5. `auth.disabled = true` (development only; see [server mode](server.md)) skips login
   entirely -- every request is an implicit administrator, and the console shows a
   standing warning banner on every page saying so.

### CSRF

Every state-changing form carries a hidden `csrf_token` field equal to the session's
CSRF secret; a POST whose field does not match is rejected with `403` before it touches
the engine or store. Since the session itself is already a `SameSite=Strict` cookie, this
is defense in depth rather than the only protection, but it also catches a stale form
(for example a page left open across a login as a different token).

### What the cookie is not

There is no server-side session table: everything needed to validate a session round-trips
through the signed cookie itself plus a lookup of the token id in the (already in-memory)
token file. This keeps the console stateless and cheap, at the cost of two things worth
knowing operationally: sessions do not survive a server restart unless `session_secret` is
set, and a session stays "logged in" against a token id even if that id's *secret* is later
rotated (only revoking or deleting the id itself invalidates the session) -- rotation
without revocation is the same trade-off the bearer tokens themselves already make for any
other long-lived credential.

## Security properties

- **Strict `Content-Security-Policy`** on every response: `default-src 'none'` plus
  narrow allowances for same-origin stylesheets and form submissions. No inline scripts
  are ever emitted (the console ships none at all), and there is nothing to load from a
  CDN -- the one stylesheet is served from `/console/static/console.css`, hand-written,
  no external fonts or scripts.
- **`X-Frame-Options: DENY`** and `frame-ancestors 'none'` in the CSP: the console cannot
  be framed.
- **`Referrer-Policy: no-referrer`** and **`X-Content-Type-Options: nosniff`**.
- **Every value is HTML-escaped.** Workflow ids, signal channel names and values, failure
  messages, schedule notes -- anything that could contain operator- or workflow-supplied
  text -- is rendered through Jinja2's autoescaping, never marked `|safe`. A workflow id
  of `<script>alert(1)</script>` (or a signal payload containing the same) shows up on
  the page as inert text, not a script tag.
- **Payloads are never dumped raw.** A run's result, a step's arguments, a history
  event's fields: each is decoded with the default converter when possible and rendered
  as a plain value; anything that cannot be decoded (compressed, encrypted, or produced
  by a codec the console does not know) renders as a placeholder describing its encoding
  and size, never as base64 ciphertext on the screen.
- **Redirect-after-POST.** Every action redirects (`303`) back to a GET, with a short
  flash message carried in the redirect target's own query string, so reloading the
  result page never resubmits the form.
- **Terminate requires an explicit confirmation checkbox** in addition to the reason
  field, since it stops a run immediately without running any more workflow code; the
  batch page's terminate action requires the same checkbox, for every matched run.

## Pages

- **Login** / **logout**.
- **Namespace picker**: every namespace the signed-in token may use, in the top nav.
- **Runs list** (`/console/{namespace}/runs`): filter by `status` and `workflow_name`, or
  give a full [visibility query](visibility-queries.md) in the query box (`status =
  'failed' AND created_at >= now() - interval '1 day'`); `limit` bounds the page size. An
  invalid query renders the parser's own pointed-at-the-token error message instead of
  failing the page.
- **Run detail** (`/console/{namespace}/runs/{run_id}`): summary fields, decoded result
  or failure (with its full cause chain), search attributes, pending step jobs (attempt,
  lease owner and expiry, last failure), children, the parent link, the continue-as-new
  chain in both directions, and the reset chain in both directions (`reset_from_run_id`
  backwards, `continued_run_id` forwards -- a reset reuses the same field continue-as-new
  does, see [recovery](operations/recovery.md#resetting-a-run)), and a full history
  timeline (event id, time, kind, and every other field of that event, decoded the same
  way results are). Below that, the signal/cancel/terminate/reset forms (`control_write`
  only): reset picks a point (first tick, before the last tick, or after a given tick
  number) and requires a reason, exactly like `loomwork workflow reset`.
- **Batch** (`/console/{namespace}/batch`): enter a
  [visibility query](visibility-queries.md), preview every run it matches (a plain read,
  `control_read`), then cancel or terminate all of them in one action (`control_write`;
  terminate needs the same confirmation checkbox as the single-run form). The preview
  *is* the confirmation step: the query text carries through the action forms so what was
  previewed is exactly what gets acted on.
- **Schedules list** and **schedule detail** (`/console/{namespace}/schedules[/...]`):
  spec, policy, next fire time, upcoming fire times, currently running and recently
  completed actions, and pause/unpause/trigger forms.

## What it does not do

The console only reads and controls what the engine and store already expose through
`Engine`/`Store` (no new store methods were added to build it): it cannot run a workflow
query handler (the control API cannot either -- the server never imports workflow code),
edit search attribute schemas (declare them in `[server.search_attributes]` instead, see
[server mode](server.md)), or create/delete/update a schedule's spec (only pause, unpause
and trigger). Use the [CLI](cli.md) or the Python client for those.
