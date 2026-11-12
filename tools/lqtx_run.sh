#!/bin/bash
# Runs the Listening Queue transcriber with everything it needs taken from this folder.
#   ./run.sh jobs/<job>            (call again until it prints DONE)
cd "$(dirname "$0")" || exit 1
export PYTHONUSERBASE="$PWD/pylib" HF_HOME="$PWD/.models" TMPDIR="$PWD/.tmp" PATH="$PWD/pylib/bin:/usr/local/bin:/usr/bin:/bin"
export HF_HUB_OFFLINE=1 TOKENIZERS_PARALLELISM=false
mkdir -p .tmp/work
exec python3 lqtx.py "$1" --budget "${2:-150}"
