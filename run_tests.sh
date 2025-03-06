#!/bin/bash

docker compose down
rm -f db/test/label_work.db
docker compose up -d
# wait for fastapi to start
sleep 7
python test/test_api.py

