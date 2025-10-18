# ADR-0005: Class-based workflows as a layer over function workflows

- Status: accepted
- Date: 2025-10-18

## Context

Function workflows declare signal, query and update handlers as nested functions that
close over local variables. That works well for one or two handlers, but workflows that
behave like long-lived entities (a subscription, a ticket, a shopping cart) end up with
most of their logic in nested closures, with state in `nonlocal` variables and no way to
reference a handler from client code except by a string name.

A class puts state in attributes and handlers in methods. The question was how deep the
change should go: new event kinds and a separate runtime path for classes, or a layer on
top of what exists.

## Decision

Class-based workflows are a pure SDK layer. `@workflow` on a class:

1. collects the `@run` method and the methods marked `@signal`, `@query` and `@update`;
2. builds an ordinary `WorkflowDefinition` whose entry function creates the instance,
   registers query and update methods through the existing `query()` and `update()`
   functions, starts one workflow task per signal that feeds the method from its
   `Channel`, and awaits `@run`;
3. attaches that definition to the class and returns **the class itself**.

`wait_condition(predicate)` is added for both styles. Its waiters are re-checked after
every signal or update handler finishes, which happens during replay exactly as it did
live, so it adds no events and needs no timers unless a timeout is given.

Every API that accepts a workflow accepts the class and resolves it with
`as_definition`. Handler methods carry a marker (and, for signals, their typed channel),
so `handle.signal(Cart.add, item)`, `handle.query(Cart.count)` and
`handle.update(Cart.checkout)` work without string names.

## Consequences

- **No history change.** A class records exactly the events of the equivalent function
  with nested handlers. Existing replay, versioning, compatibility checks and the linter
  apply unchanged, and a workflow can move between the two styles without breaking runs
  in flight, as long as its commands stay the same.
- **Type checkers see a class.** mypy and pyright ignore what class decorators return, so
  returning a definition object would have made every attribute access on the workflow a
  type error. Returning the class keeps IDE navigation and checking of handler methods
  intact; the price is that APIs accept `type` and check at runtime.
- **Subclasses are separate workflows.** The definition is looked up in the class's own
  namespace, not inherited, so a subclass must be decorated itself and gets its own name.
- **Fingerprints cover the whole class.** The code version recorded with each tick
  hashes every method and its handler role, because a change to any handler can change
  what the workflow decides.
- **Handler concurrency is visible.** Signal messages are handled in order per signal,
  but an async handler that awaits a step can still be running when the next update
  arrives. This matches the function style and is documented rather than hidden behind
  implicit locking, which would make handler order depend on step timing.
