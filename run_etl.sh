#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VENV_PATH="${SCRIPT_DIR}/venv/bin/activate"
source $VENV_PATH

PYTHON_SCRIPT="${SCRIPT_DIR}/etl.py"

python "$PYTHON_SCRIPT" >> "${SCRIPT_DIR}/log/logfile.log" 2>&1

dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi Started at ${dt}" >> "${SCRIPT_DIR}/log/luigi-info.log"