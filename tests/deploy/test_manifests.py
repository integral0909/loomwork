"""Checks on the deployment kit in ``deploy/``.

The manifests are configuration, but they encode claims the documentation makes: every
container runs unprivileged on a read-only filesystem, every command is a valid
``loomwork`` invocation, secrets never appear in plain text, and images are pinned. These
tests keep those claims true as the CLI and the manifests evolve.
"""

from __future__ import annotations

import re
import shlex
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from loomwork.cli.main import build_parser
from loomwork.server.config import load_config

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
KUBERNETES = DEPLOY / "kubernetes"
COMPOSE = DEPLOY / "compose" / "compose.yaml"
DOCKERFILE = DEPLOY / "docker" / "Dockerfile"

IMAGE_TAG = "0.1.0"
UID = 10001


def documents(path: Path) -> list[dict[str, Any]]:
    return [doc for doc in yaml.safe_load_all(path.read_text()) if doc]


def kubernetes_objects() -> list[dict[str, Any]]:
    kustomization = yaml.safe_load((KUBERNETES / "kustomization.yaml").read_text())
    objects: list[dict[str, Any]] = []
    for resource in kustomization["resources"]:
        objects.extend(documents(KUBERNETES / resource))
    return objects


def pod_specs() -> Iterator[tuple[str, dict[str, Any]]]:
    for obj in kubernetes_objects():
        kind = obj["kind"]
        name = obj["metadata"]["name"]
        if kind == "Deployment" or kind == "Job":
            yield name, obj["spec"]["template"]["spec"]
        elif kind == "CronJob":
            yield name, obj["spec"]["jobTemplate"]["spec"]["template"]["spec"]


def containers() -> Iterator[tuple[str, dict[str, Any], dict[str, Any]]]:
    for name, spec in pod_specs():
        for container in spec["containers"]:
            yield name, spec, container


def compose() -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load(COMPOSE.read_text())
    return data


def compose_services() -> dict[str, dict[str, Any]]:
    services: dict[str, dict[str, Any]] = compose()["services"]
    return services


# --------------------------------------------------------------------------- kubernetes


def test_kustomization_lists_every_manifest_except_the_secret_template() -> None:
    listed = set(yaml.safe_load((KUBERNETES / "kustomization.yaml").read_text())["resources"])
    on_disk = {p.name for p in KUBERNETES.glob("*.yaml")} - {"kustomization.yaml"}
    assert on_disk - listed == {"credentials.example.yaml"}


def test_expected_objects() -> None:
    kinds = sorted((o["kind"], o["metadata"]["name"]) for o in kubernetes_objects())
    assert kinds == [
        ("ConfigMap", "loomwork-server"),
        ("CronJob", "loomwork-retention"),
        ("Deployment", "loomwork-server"),
        ("Deployment", "loomwork-worker"),
        ("Job", "loomwork-migrate"),
        ("Namespace", "loomwork"),
        ("NetworkPolicy", "loomwork-server"),
        ("NetworkPolicy", "loomwork-worker"),
        ("PodDisruptionBudget", "loomwork-server"),
        ("PodDisruptionBudget", "loomwork-worker"),
        ("Service", "loomwork-server"),
    ]


def test_namespace_enforces_restricted_pod_security() -> None:
    (namespace,) = (o for o in kubernetes_objects() if o["kind"] == "Namespace")
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"


@pytest.mark.parametrize(("workload", "spec"), list(pod_specs()))
def test_pods_run_unprivileged(workload: str, spec: dict[str, Any]) -> None:
    security = spec["securityContext"]
    assert security["runAsNonRoot"] is True
    assert security["runAsUser"] == UID
    assert security["seccompProfile"]["type"] == "RuntimeDefault"
    assert spec["automountServiceAccountToken"] is False


@pytest.mark.parametrize(
    ("workload", "container"), [(w, c) for w, _, c in containers()], ids=lambda v: str(v)[:40]
)
def test_containers_are_hardened_and_bounded(workload: str, container: dict[str, Any]) -> None:
    security = container["securityContext"]
    assert security["allowPrivilegeEscalation"] is False
    assert security["readOnlyRootFilesystem"] is True
    assert security["capabilities"]["drop"] == ["ALL"]
    resources = container["resources"]
    for kind in ("requests", "limits"):
        assert {"cpu", "memory"} <= set(resources[kind]), f"{workload}: {kind}"


def test_images_are_pinned_to_one_release() -> None:
    for workload, _, container in containers():
        image = container["image"]
        assert image.rsplit(":", 1)[1] == IMAGE_TAG, workload
        assert not image.endswith(":latest")
    kustomization = yaml.safe_load((KUBERNETES / "kustomization.yaml").read_text())
    assert {i["newTag"] for i in kustomization["images"]} == {IMAGE_TAG}


def test_long_running_containers_have_probes() -> None:
    for workload, _, container in containers():
        if workload in {"loomwork-server", "loomwork-worker"}:
            assert "readinessProbe" in container, workload
            assert "livenessProbe" in container, workload


def test_worker_grace_period_outlasts_its_shutdown_grace() -> None:
    (spec,) = (s for name, s in pod_specs() if name == "loomwork-worker")
    args = spec["containers"][0]["args"]
    grace = args[args.index("--shutdown-grace") + 1]
    from loomwork.cli.parsing import parse_duration

    assert spec["terminationGracePeriodSeconds"] > parse_duration(grace).total_seconds()


@pytest.mark.parametrize(
    ("workload", "container"), [(w, c) for w, _, c in containers()], ids=lambda v: str(v)[:40]
)
def test_container_commands_are_valid_cli_invocations(
    workload: str, container: dict[str, Any]
) -> None:
    args = build_parser().parse_args(container["args"])
    assert callable(args.handler), workload


def test_secret_references_match_the_template() -> None:
    template = {
        doc["metadata"]["name"]: set(doc["stringData"])
        for doc in documents(KUBERNETES / "credentials.example.yaml")
    }
    for workload, spec, container in containers():
        for env in container.get("env", []):
            ref = env.get("valueFrom", {}).get("secretKeyRef")
            if ref:
                assert ref["key"] in template[ref["name"]], workload
        for volume in spec.get("volumes", []):
            if "secret" in volume:
                assert volume["secret"]["secretName"] in template, workload


def test_server_config_map_loads_and_points_at_the_mounted_tokens(tmp_path: Path) -> None:
    (config_map,) = (o for o in kubernetes_objects() if o["kind"] == "ConfigMap")
    toml = config_map["data"]["server.toml"]
    (server,) = (s for name, s in pod_specs() if name == "loomwork-server")
    mounts = {m["name"]: m["mountPath"] for m in server["containers"][0]["volumeMounts"]}
    (tmp_path / "server.toml").write_text(toml)
    config = load_config(
        tmp_path / "server.toml", env={"LOOMWORK_SERVER__NAMESPACES__DEFAULT": "postgresql://db/x"}
    )
    assert config.namespaces == {"default": "postgresql://db/x"}
    assert config.auth.tokens_file is not None
    # Mounted as a directory, so Secret updates (and token reloads) reach the pod.
    assert str(config.auth.tokens_file.parent) == mounts["tokens"]
    assert config.port == server["containers"][0]["ports"][0]["containerPort"]


def test_no_plaintext_credentials_outside_the_template() -> None:
    credential = re.compile(r"postgres(?:ql)?://[^:@/\s]+:(?!\$\{)[^@\s]+@")
    for path in [*KUBERNETES.glob("*.yaml"), COMPOSE, DEPLOY / "compose" / "server.toml"]:
        if path.name == "credentials.example.yaml":
            assert "REPLACE_ME" in path.read_text()
            continue
        assert not credential.search(path.read_text()), path
        assert "lwk_" not in path.read_text(), path


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl not installed")
def test_kustomize_renders() -> None:
    rendered = subprocess.run(
        ["kubectl", "kustomize", str(KUBERNETES)], capture_output=True, text=True, check=True
    ).stdout
    objects = [doc for doc in yaml.safe_load_all(rendered) if doc]
    assert len(objects) == len(kubernetes_objects())
    assert {o["metadata"].get("namespace", "loomwork") for o in objects} == {"loomwork"}


# --------------------------------------------------------------------------- compose


def test_compose_services_and_ordering() -> None:
    services = compose_services()
    assert set(services) == {"postgres", "migrate", "tokens", "server", "worker"}
    server_deps = services["server"]["depends_on"]
    assert server_deps["migrate"]["condition"] == "service_completed_successfully"
    assert server_deps["tokens"]["condition"] == "service_completed_successfully"
    assert services["worker"]["depends_on"]["server"]["condition"] == "service_healthy"


def test_compose_commands_are_valid_cli_invocations() -> None:
    parser = build_parser()
    for name in ("migrate", "server", "worker"):
        args = parser.parse_args(compose_services()[name]["command"])
        assert callable(args.handler), name
    script = compose_services()["tokens"]["command"][0]
    for invocation in script.split("&&"):
        words = shlex.split(invocation)
        assert words[0] == "loomwork"
        assert parser.parse_args(words[1:]).if_missing is True


def test_compose_requires_a_database_password() -> None:
    text = COMPOSE.read_text()
    assert "${POSTGRES_PASSWORD:?" in text
    assert "POSTGRES_PASSWORD: ${POSTGRES_PASSWORD" in text


def test_compose_loomwork_containers_are_hardened() -> None:
    for name, service in compose_services().items():
        if name == "postgres":
            continue
        assert service["read_only"] is True, name
        assert service["cap_drop"] == ["ALL"], name
        assert "no-new-privileges:true" in service["security_opt"], name


def test_compose_ports_bind_to_localhost() -> None:
    for name, service in compose_services().items():
        for port in service.get("ports", []):
            assert port.startswith("127.0.0.1:"), name


def test_compose_server_config_loads() -> None:
    config = load_config(
        DEPLOY / "compose" / "server.toml",
        env={"LOOMWORK_SERVER__NAMESPACES__DEFAULT": "postgresql://postgres/loomwork"},
    )
    assert config.host == "0.0.0.0"
    assert config.auth.tokens_file is not None
    token_mount = next(
        v for v in compose_services()["server"]["volumes"] if v.startswith("secrets:")
    )
    assert str(config.auth.tokens_file.parent) == token_mount.split(":")[1]


def test_worker_stop_grace_outlasts_shutdown_grace() -> None:
    from loomwork.cli.parsing import parse_duration

    worker = compose_services()["worker"]
    command = worker["command"]
    grace = parse_duration(command[command.index("--shutdown-grace") + 1])
    assert parse_duration(worker["stop_grace_period"]) > grace


# --------------------------------------------------------------------------- dockerfile


def test_dockerfile_runs_as_the_manifest_uid() -> None:
    text = DOCKERFILE.read_text()
    assert f"USER {UID}:{UID}" in text
    assert f"--uid {UID}" in text
    assert "HEALTHCHECK" in text
    assert re.search(r"^ARG PYTHON_VERSION=3\.12\.\d+$", text, re.MULTILINE)


def test_dockerfile_default_command_is_valid() -> None:
    match = re.search(r"^CMD (\[.*\])$", DOCKERFILE.read_text(), re.MULTILINE)
    assert match is not None
    args = yaml.safe_load(match.group(1))
    assert callable(build_parser().parse_args(args).handler)


def test_dockerignore_keeps_the_build_context_small() -> None:
    ignored = (DEPLOY.parent / ".dockerignore").read_text().split()
    for entry in (".git", ".venv", "tests", "**/__pycache__"):
        assert entry in ignored
