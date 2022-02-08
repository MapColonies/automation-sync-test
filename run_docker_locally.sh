#!/bin/bash

docker run -ti \
--net=host \
-e PYTEST_RUNNING_MODE=watcher \
-e CONF_FILE="/tmp/sync/configuration.json" \
-e AGENT_URL="http://discrete-agent-qa-agent-route-raster.apps.v0h0bdx6.eastus.aroapp.io/" \
-v /tmp/sync:/tmp/sync \
automation-sync-test:latest


# PYTEST_RUNNING_MODE=e2e | e2e_sender| e2e_receiver | ingest_only | watcher
