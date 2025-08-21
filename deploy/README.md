# Deployment kit

Everything needed to run Loomwork in containers:

| Path | What it is |
| ---- | ---------- |
| [`docker/Dockerfile`](docker/Dockerfile) | Multi-stage image: `runtime` (Loomwork with server, PostgreSQL, SQLite and encryption extras) and `demo` (plus the example apps) |
| [`compose/`](compose/) | PostgreSQL, a migration step, token bootstrap, the server and a worker, for local use |
| [`kubernetes/`](kubernetes/) | Kustomize base: server Deployment/Service/PDB, worker Deployment/PDB, migration Job, retention CronJob, NetworkPolicies, restricted Pod Security |

`tests/deploy/` checks these files against the code: every container command is parsed by
the real `loomwork` CLI, the server configuration is loaded by the real config loader,
every container is unprivileged with a read-only root filesystem and resource limits, and
no credentials appear outside the secret template.

## Images

```bash
docker build -f deploy/docker/Dockerfile -t loomwork:0.1.0 .
```

The image runs as uid `10001`, has no shell tools beyond the Debian slim base, and starts
`loomwork server --config /etc/loomwork/server.toml` by default. Its entry point is the
`loomwork` command, so any subcommand works: `docker run loomwork:0.1.0 store migrate`.

Your workers need your workflow code. Build on the Loomwork image:

```dockerfile
FROM ghcr.io/integral0909/loomwork:0.1.0
COPY --chown=10001:10001 myapp /opt/app/myapp
ENV PYTHONPATH=/opt/app
CMD ["worker", "--app", "myapp.workflows", "--server", "http://loomwork-server:8080", \
     "--metrics-port", "9464", "--metrics-host", "0.0.0.0"]
```

If your steps need extra packages, install them into `/opt/loomwork` in a build stage the
same way the Dockerfile installs Loomwork.

## Compose

```bash
export POSTGRES_PASSWORD=$(openssl rand -hex 16)
docker compose -f deploy/compose/compose.yaml up --build
```

Start order is enforced: PostgreSQL becomes healthy, `migrate` applies the schema,
`tokens` creates the tokens file and the worker and admin tokens in the `secrets`
volume (only if they do not exist yet), the server starts and becomes ready, and then
the worker connects to it over HTTP with its token. The server listens on
`127.0.0.1:8080` and the worker's metrics on `127.0.0.1:9464`.

## Kubernetes

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
# create the three Secrets (see kubernetes/secrets.example.yaml)
kubectl apply -k deploy/kubernetes
```

- **Server**: two replicas behind a Service, readiness on `/readyz` (every namespace's
  store answers), liveness on `/healthz`, a PodDisruptionBudget keeping one up. The
  store URL comes from a Secret through `LOOMWORK_SERVER__NAMESPACES__DEFAULT`, never
  from the ConfigMap. Tokens are a Secret mounted as a directory, so rotating or revoking
  a token (`kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`)
  reaches running servers, which reload the file within a second.
- **Workers**: talk only to the server, with a `worker` token; they hold no database
  credentials. `terminationGracePeriodSeconds` exceeds `--shutdown-grace`, so a rolling
  update lets in-flight steps finish or hands them to another worker. Prometheus scrape
  annotations point at the metrics port.
- **Migration Job**: run it before each rollout (a PreSync or pre-upgrade hook), so a
  failing migration stops the deploy.
- **Retention CronJob**: `loomwork store prune --older-than 30d` nightly.
- **NetworkPolicies**: the server accepts traffic from pods in the namespace and reaches
  only DNS and PostgreSQL; workers reach the server, DNS and HTTPS. Tighten the worker
  egress to what your steps call.

Before `loomwork compat` gates your deploys, run it in CI against a read replica or a
recent history export (see [versioning](../docs/guides/versioning.md)).
