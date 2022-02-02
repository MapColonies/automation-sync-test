#!/bin/bash

docker run -ti \
--net=host \
-e PYTEST_RUNNING_MODE=watcher \
-e CONF_FILE="/tmp/sync/configuration.json" \
-e agent_url="http://discrete-agent-qa-agent-route-raster.apps.v0h0bdx6.eastus.aroapp.io/" \
-v /tmp/sync:/tmp/sync \
automation-sync-test:latest
