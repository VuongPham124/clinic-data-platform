# import json

# def load_contract(path: str) -> dict:
#     with open(path, "r", encoding="utf-8") as f:
#         return json.load(f)

import json
import os
import subprocess
from pathlib import Path


def load_contract(path: str) -> dict:
    """
    ADDED: Support reading contract from both local path and gs:// URI.
    - If path starts with gs://, download to /tmp then open().
    - Else, open local file directly.
    """
    local_path = path

    # --- ADDED: handle GCS URI ---
    if isinstance(path, str) and path.startswith("gs://"):
        tmp_dir = Path("/tmp/contracts")
        tmp_dir.mkdir(parents=True, exist_ok=True)

        filename = path.split("/")[-1] or "contract.json"
        local_path = str(tmp_dir / filename)

        # Download only if not exists (or you can always overwrite)
        if not os.path.exists(local_path):
            subprocess.run(
                ["gsutil", "-q", "cp", path, local_path],
                check=True,
            )
    # -----------------------------

    with open(local_path, "r", encoding="utf-8") as f:
        return json.load(f)
