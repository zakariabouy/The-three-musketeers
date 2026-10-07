#!/bin/bash
set -euo pipefail

if [ -f "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install --user -r /opt/airflow/requirements.txt
fi

airflow db migrate

if ! airflow users list | grep -q '^admin\s'; then
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

exec airflow webserver