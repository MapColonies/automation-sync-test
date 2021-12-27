#!/bin/bash

docker run \
--net=host \
-e PYTEST_RUNNING_MODE=e2e \
-e CONF_FILE="/tmp/sync/configuration.json" \
-v /tmp/sync:/tmp/sync \
automation-sync-test:latest
