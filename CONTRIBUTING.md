# Contributing to Loomwork

## Development setup

Loomwork uses [uv](https://docs.astral.sh/uv/) and targets Python 3.12+.

```bash
uv sync                    # create .venv with runtime and dev dependencies
uv run pytest              # run the test suite
uv run ruff check .        # lint
uv run ruff format .       # format
uv run mypy                # strict type-check
```

Tests that need PostgreSQL are skipped unless `LOOMWORK_TEST_POSTGRES_DSN` is set:

```bash
docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=loomwork postgres:16
export LOOMWORK_TEST_POSTGRES_DSN=postgresql://postgres:loomwork@localhost:5432/postgres
```

## Ground rules

- **The event history is a public contract.** Any change to an event type, its fields, or
  how commands map onto events must stay replay-compatible with existing histories.
  The histories in `tests/replay_golden/histories/` must keep decoding, re-encoding
  byte for byte and replaying; a new event kind needs a scenario in
  `tests/replay_golden/record.py` that produces it (the suite fails until one does).
  Record new files with `python -m tests.replay_golden.record`; never re-record an
  existing one to make a failing test pass.
- **Every store operation is atomic.** New store methods must be implemented for every
  backend and covered by the shared store conformance suite.
- **Workflow-side code must stay deterministic.** Anything under `loomwork.runtime` and
  `loomwork.sdk` that runs inside a workflow may not read wall-clock time, randomness,
  or perform I/O.
- Keep changes focused; one logical change per pull request.
- Documentation is tested too: links, anchors and Python snippets in every Markdown file
  are checked by `tests/docs/`. [docs/internals.md](docs/internals.md) explains where
  each kind of change belongs.
- Architectural decisions are recorded as ADRs in `docs/adr/`.

## Commit messages

Use the imperative mood ("Add timer cancellation", not "Added ..."), keep the subject
under 72 characters, and explain *why* in the body when it is not obvious.
