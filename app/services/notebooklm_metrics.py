from __future__ import annotations

from threading import Lock

_READY_REASONS = (
    "ok",
    "auth_expired",
    "tunnel_down:telegram",
    "tunnel_down:notebooklm",
    "sync_stale",
    "storage_missing",
    "not_enabled",
)

_HELP = {
    "nlm_ready": "NotebookLM readiness state by reason.",
    "nlm_tunnel_up": "NotebookLM tunnel reachability by tunnel name.",
    "nlm_storage_state_age_seconds": "Age of the active NotebookLM storage_state file in seconds.",
    "nlm_sources_used": "Configured rolling NotebookLM source count per notebook.",
    "nlm_sync_last_success_timestamp": "Unix timestamp of the last successful NotebookLM sync.",
    "nlm_auth_expired_total": "Total count of NotebookLM auth-expired incidents observed by the runtime.",
    "nlm_handler_exception_total": "Total count of uncaught bot handler exceptions.",
}

_TYPE = {
    "nlm_ready": "gauge",
    "nlm_tunnel_up": "gauge",
    "nlm_storage_state_age_seconds": "gauge",
    "nlm_sources_used": "gauge",
    "nlm_sync_last_success_timestamp": "gauge",
    "nlm_auth_expired_total": "counter",
    "nlm_handler_exception_total": "counter",
}

_state_lock = Lock()
_gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
_counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}


def _labels_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(key), str(value)) for key, value in labels.items()))


def _quote_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def set_gauge(name: str, value: float, **labels: str) -> None:
    with _state_lock:
        _gauges[(name, _labels_key(labels))] = float(value)


def inc_counter(name: str, amount: float = 1.0, **labels: str) -> None:
    with _state_lock:
        key = (name, _labels_key(labels))
        _counters[key] = _counters.get(key, 0.0) + float(amount)


def set_readiness(reason: str) -> None:
    normalized_reason = reason if reason in _READY_REASONS else "ok"
    for candidate in _READY_REASONS:
        set_gauge("nlm_ready", 1.0 if candidate == normalized_reason else 0.0, reason=candidate)


def set_tunnel_up(which: str, up: bool) -> None:
    set_gauge("nlm_tunnel_up", 1.0 if up else 0.0, which=which)


def set_storage_state_age_seconds(value: float) -> None:
    set_gauge("nlm_storage_state_age_seconds", value)


def set_sources_used(notebook_id: str, used: int) -> None:
    if not notebook_id:
        return
    set_gauge("nlm_sources_used", float(used), notebook_id=notebook_id)


def set_sync_last_success_timestamp(timestamp_s: float) -> None:
    set_gauge("nlm_sync_last_success_timestamp", timestamp_s)


def inc_auth_expired_total() -> None:
    inc_counter("nlm_auth_expired_total")


def inc_handler_exception_total() -> None:
    inc_counter("nlm_handler_exception_total")


def render_prometheus_text() -> str:
    with _state_lock:
        gauges = dict(_gauges)
        counters = dict(_counters)

    lines: list[str] = []
    for name in _HELP:
        lines.append(f"# HELP {name} {_HELP[name]}")
        lines.append(f"# TYPE {name} {_TYPE[name]}")
        samples: list[tuple[tuple[str, tuple[tuple[str, str], ...]], float]] = []
        samples.extend((key, value) for key, value in gauges.items() if key[0] == name)
        samples.extend((key, value) for key, value in counters.items() if key[0] == name)
        if not samples and name == "nlm_ready":
            samples.append(((name, _labels_key({"reason": "ok"})), 0.0))
        for (sample_name, labels), value in sorted(samples):
            if labels:
                rendered_labels = ",".join(f'{key}="{_quote_label(label)}"' for key, label in labels)
                lines.append(f"{sample_name}{{{rendered_labels}}} {value}")
            else:
                lines.append(f"{sample_name} {value}")
    return "\n".join(lines) + "\n"
