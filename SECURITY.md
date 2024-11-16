# Security Policy

## Supported versions

Loomwork is pre-1.0. Security fixes are released for the latest minor version only.

## Reporting a vulnerability

Please do **not** open a public issue for security problems.

Use GitHub's private vulnerability reporting on
<https://github.com/integral0909/loomwork/security/advisories/new>, or email
fredrik.fullstackdev@gmail.com with the subject line `loomwork security`.

Include the affected version, the store backend, and a description or proof of concept.
You should receive an acknowledgement within five working days. Fixes are coordinated
privately and disclosed together with a patched release.

## Security model

- **Workflow payloads are opaque to the engine.** Arguments, results and failure details
  pass through the configured payload codec chain. When encryption is enabled, the engine
  and store only ever see ciphertext.
- **Workflow code is trusted code.** Loomwork does not sandbox workflow or step functions;
  the determinism guards exist for correctness, not isolation.
- **Server mode requires authentication.** API tokens are stored hashed and scoped to
  namespaces with role-based permissions.
- **No secrets in configuration files.** Credentials are read from the environment or
  from files referenced by path.
