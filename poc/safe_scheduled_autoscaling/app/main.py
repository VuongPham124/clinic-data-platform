import json
import os
from typing import Any, Dict, Tuple

import google.auth
from google.auth.transport.requests import Request
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

COMPOSER_API = "https://composer.googleapis.com/v1"
UPDATE_MASK = (
    "config.workloadsConfig.worker.minCount,"
    "config.workloadsConfig.worker.maxCount,"
    "config.workloadsConfig.worker.cpu,"
    "config.workloadsConfig.worker.memoryGb,"
    "config.workloadsConfig.worker.storageGb"
)

PROJECT_ID = os.environ["PROJECT_ID"]
LOCATION = os.environ["LOCATION"]
COMPOSER_ENV = os.environ["COMPOSER_ENV"]
BASELINE_PATH = os.environ.get("BASELINE_PATH", "/tmp/worker-baseline.json")

DEFAULT_DOWNSCALE = {
    "minCount": int(os.environ.get("DOWNSCALE_MIN_COUNT", "1")),
    "maxCount": int(os.environ.get("DOWNSCALE_MAX_COUNT", "1")),
    "cpu": float(os.environ.get("DOWNSCALE_CPU", "0.5")),
    "memoryGb": float(os.environ.get("DOWNSCALE_MEMORY_GB", "2")),
    "storageGb": float(os.environ.get("DOWNSCALE_STORAGE_GB", "1")),
}

DEFAULT_RESTORE = {
    "minCount": int(os.environ.get("RESTORE_MIN_COUNT", "2")),
    "maxCount": int(os.environ.get("RESTORE_MAX_COUNT", "6")),
    "cpu": float(os.environ.get("RESTORE_CPU", "1")),
    "memoryGb": float(os.environ.get("RESTORE_MEMORY_GB", "3.75")),
    "storageGb": float(os.environ.get("RESTORE_STORAGE_GB", "20")),
}


def _env_name() -> str:
    return f"projects/{PROJECT_ID}/locations/{LOCATION}/environments/{COMPOSER_ENV}"


def _auth_headers() -> Dict[str, str]:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(Request())
    return {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}


def _composer_get_environment() -> Dict[str, Any]:
    url = f"{COMPOSER_API}/{_env_name()}"
    resp = requests.get(url, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def _extract_worker(environment: Dict[str, Any]) -> Dict[str, Any]:
    return (
        environment.get("config", {})
        .get("workloadsConfig", {})
        .get("worker", {})
    )


def _save_baseline(worker: Dict[str, Any]) -> None:
    data = {
        "minCount": worker.get("minCount"),
        "maxCount": worker.get("maxCount"),
        "cpu": worker.get("cpu"),
        "memoryGb": worker.get("memoryGb"),
        "storageGb": worker.get("storageGb"),
    }
    with open(BASELINE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _load_baseline() -> Dict[str, Any]:
    with open(BASELINE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _state_guard(environment: Dict[str, Any]) -> Tuple[bool, str]:
    state = environment.get("state", "UNKNOWN")
    if state == "UPDATING":
        return False, "Composer environment is UPDATING; skip patch to avoid overlap"
    return True, "ok"


def _patch_worker(target_worker: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{COMPOSER_API}/{_env_name()}?updateMask={UPDATE_MASK}"
    body = {
        "config": {
            "workloadsConfig": {
                "worker": {
                    "minCount": int(target_worker["minCount"]),
                    "maxCount": int(target_worker["maxCount"]),
                    "cpu": float(target_worker["cpu"]),
                    "memoryGb": float(target_worker["memoryGb"]),
                    "storageGb": float(target_worker["storageGb"]),
                }
            }
        }
    }
    resp = requests.patch(url, headers=_auth_headers(), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _merge_overrides(defaults: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    out = defaults.copy()
    for k in ["minCount", "maxCount", "cpu", "memoryGb", "storageGb"]:
        if k in payload:
            out[k] = payload[k]
    return out


def _validate_worker(worker: Dict[str, Any]) -> Tuple[bool, str]:
    if int(worker["minCount"]) > int(worker["maxCount"]):
        return False, "minCount must be <= maxCount"
    if float(worker["cpu"]) <= 0 or float(worker["memoryGb"]) <= 0 or float(worker["storageGb"]) <= 0:
        return False, "cpu, memoryGb, storageGb must be > 0"
    return True, "ok"


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "environment": _env_name()})


@app.post("/downscale")
def downscale():
    payload = request.get_json(silent=True) or {}
    dry_run = bool(payload.get("dryRun", False))

    env = _composer_get_environment()
    ok, reason = _state_guard(env)
    if not ok:
        return jsonify({"ok": False, "reason": reason}), 409

    current_worker = _extract_worker(env)

    target = _merge_overrides(DEFAULT_DOWNSCALE, payload)
    valid, reason = _validate_worker(target)
    if not valid:
        return jsonify({"ok": False, "reason": reason}), 400

    if dry_run:
        return jsonify(
            {
                "ok": True,
                "dryRun": True,
                "current": current_worker,
                "target": target,
                "updateMask": UPDATE_MASK,
            }
        )

    _save_baseline(current_worker)
    op = _patch_worker(target)
    return jsonify({"ok": True, "action": "downscale", "operation": op, "target": target})


@app.post("/restore")
def restore():
    payload = request.get_json(silent=True) or {}
    dry_run = bool(payload.get("dryRun", False))
    use_baseline = bool(payload.get("useBaseline", False))

    env = _composer_get_environment()
    ok, reason = _state_guard(env)
    if not ok:
        return jsonify({"ok": False, "reason": reason}), 409

    source = "defaultRestore"
    base = DEFAULT_RESTORE.copy()
    if use_baseline:
        try:
            base = _load_baseline()
            source = "baseline"
        except FileNotFoundError:
            base = DEFAULT_RESTORE.copy()
            source = "defaultRestoreFallback"

    target = _merge_overrides(base, payload)
    valid, reason = _validate_worker(target)
    if not valid:
        return jsonify({"ok": False, "reason": reason}), 400

    if dry_run:
        return jsonify(
            {
                "ok": True,
                "dryRun": True,
                "current": _extract_worker(env),
                "target": target,
                "source": source,
                "updateMask": UPDATE_MASK,
            }
        )

    op = _patch_worker(target)
    return jsonify(
        {
            "ok": True,
            "action": "restore",
            "source": source,
            "operation": op,
            "target": target,
        }
    )


@app.errorhandler(requests.HTTPError)
def handle_http_error(err: requests.HTTPError):
    response = err.response
    detail = None
    if response is not None:
        try:
            detail = response.json()
        except ValueError:
            detail = response.text
    return jsonify({"ok": False, "error": str(err), "detail": detail}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
