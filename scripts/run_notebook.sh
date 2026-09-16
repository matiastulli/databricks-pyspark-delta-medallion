#!/usr/bin/env bash
# Uploads one notebook from notebooks/ to the workspace and runs it once on serverless compute.
#
# A stopgap until step 5, where a Databricks Asset Bundle deploys and runs the whole pipeline as a job.
# `databricks jobs submit` creates a one-time run: no job definition is saved in the workspace, and a
# task with no cluster settings runs on serverless (the only compute on Free Edition).
#
# Usage: scripts/run_notebook.sh 00_bronze   (after `databricks auth login`)
# Settings come from .env when it exists (see .env.example); the defaults below apply otherwise.
set -euo pipefail

NOTEBOOK="${1:?usage: scripts/run_notebook.sh <notebook name, e.g. 00_bronze>}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE="$REPO_ROOT/notebooks/$NOTEBOOK.py"
[[ -f "$SOURCE" ]] || { echo "no such notebook: $SOURCE" >&2; exit 1; }

if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a; source "$REPO_ROOT/.env"; set +a
fi

# The workspace folder is under the logged-in user, looked up rather than committed.
USER_NAME=$(databricks current-user me -o json | python3 -c 'import json, sys; print(json.load(sys.stdin)["userName"])')
WORKSPACE_DIR="/Users/$USER_NAME/databricks-pyspark-delta-medallion/notebooks"

echo "-> uploading $NOTEBOOK to $WORKSPACE_DIR"
databricks workspace mkdirs "$WORKSPACE_DIR"
databricks workspace import "$WORKSPACE_DIR/$NOTEBOOK" --file "$SOURCE" --language PYTHON --format SOURCE --overwrite

# Every layer's names go to every notebook; each notebook reads only the widgets it defines.
payload=$(python3 - "$NOTEBOOK" "$WORKSPACE_DIR/$NOTEBOOK" <<'EOF'
import json, os, sys
name, path = sys.argv[1], sys.argv[2]
params = {
    "catalog": os.environ.get("CATALOG", "medallion"),
    "bronze_schema": os.environ.get("BRONZE_SCHEMA", "00_bronze"),
    "silver_schema": os.environ.get("SILVER_SCHEMA", "01_silver"),
    "gold_schema": os.environ.get("GOLD_SCHEMA", "02_gold"),
}
print(json.dumps({
    "run_name": f"manual {name}",
    "tasks": [{
        "task_key": name,
        "notebook_task": {"notebook_path": path, "source": "WORKSPACE", "base_parameters": params},
    }],
}))
EOF
)

echo "-> running $NOTEBOOK on serverless (waits until the run finishes)"
run=$(databricks jobs submit --timeout 30m -o json --json "$payload")

python3 - "$run" <<'EOF'
import json, subprocess, sys
run = json.loads(sys.argv[1])
state = run["state"]
print("   run page:", run.get("run_page_url"))
print("   result:  ", state.get("result_state"), state.get("state_message", ""))
task_run_id = str(run["tasks"][0]["run_id"])
output = json.loads(subprocess.run(
    ["databricks", "jobs", "get-run-output", task_run_id, "-o", "json"],
    check=True, capture_output=True, text=True,
).stdout)
if output.get("error"):
    print("   error:   ", output["error"])
if output.get("notebook_output", {}).get("result"):
    print("   output:  ", output["notebook_output"]["result"])
sys.exit(state.get("result_state") != "SUCCESS")
EOF
