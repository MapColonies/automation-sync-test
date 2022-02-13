[![Python 3.6](https://img.shields.io/badge/python-3.6-green.svg)](https://www.python.org/downloads/release/python-360/)
<img alt="GitHub release (latest by date including pre-releases)" src="https://img.shields.io/github/v/release/MapColonies/automation-sync-test">
![GitHub](https://img.shields.io/github/license/MapColonies/automation-ingestion-test)
<img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/MapColonies/automation-sync-test">
# automation-sync-test
This is full tester infrastructures for testing Map Colonies synchronization services with several test kind as will be described.

## Test suite include following tests:
1. Synchronization of core A:
    1. Create new layer ingestion
    2. Trigger \ Follow sync process -> sender -> send layer to other core \ GW 
    3. Metadata validation + storage validation + sending all data validation
2. Synchronization of core B:
    1. Listen job manger and receive configurable layer (provide Product ID + Product Version)
    2. Follow tile receiving and toc 
    3. Run layer creation validation -> PYCSW + MAPPROXY
3. Full cycle A to B (Only on Azure env -> no GW) -> combine test flows of [1] and [2]
4. Agent API CLI utils (by providing running configuration)

## Run & Deployment:
1. This repo can run tests as:
    1. Local package
    2. Docker

#### *Local* run + deploy: <br />
##### Environment specification:
* os: Ubuntu 18.04 or higher
* python: >=3.6.8
* GDAL external plugin library installed:
             ``wget http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz &&
               tar -xvzf spatialindex-src-1.8.5.tar.gz &&
               cd spatialindex-src-1.8.5 &&
               ./configure &&
               make &&
               make install &&
               cd - &&
               rm -rf spatialindex-src-1.8.5* &&
               ldconfig``
1. Download repo: [cmd prompt] `git clone https://github.com/MapColonies/automation-sync-test.git`
2. Create venv inside the project [cmd prompt] `python -m venv venv`
3. Activate the venv: [cmd prompt] `source venv/bin/activate`
4. Run installation of framework [cmd prompt] `pip install .`
5. Run some test\s from tests dir [cmd prompt] `pytest sync_tester/tests/<for specific module mention the module name`:
    * Provide configuration file + relevant environment variables before running -> will be describe later
    * test_sync_e2e_azure.py -> Cross Cores End-To-End , from core A to core B
    * test_trigger_sync_core.py -> For End-To-End only on core A (sender)
    * test_receive_sync_core.py -> For End-To-End only on core B (receive)
    * test_run_only_ingestion.py -> Execute basic sanity only ingestion on core A
6. For running Discrete agent CLI -> [cmd prompt] `python sync_tester/watch_cli/watcher.py`


#### *Docker* run + deploy: <br />
##### Environment specification:
* os: Ubuntu 18.04 or higher
* Docker installed

1. Download repo: [cmd prompt] `git clone https://github.com/MapColonies/automation-sync-test.git`
2. *Building new image* - Run from project directory [sync_tester] -> [cmd prompt] `./build.sh`
3. *Running docker locally* - use sample script or run docker individual: [cmd prompt] `./run_docker_locally.sh`:
    * Validate configure the *run_docker_locally.sh* with right configuration and mode
    * Running Docker mode [``-e PYTEST_RUNNING_MODE``]:
        * `e2e` -> Cross Cores End-To-End , from core A to core B
        * `e2e_sender` -> For End-To-End only on core A (sender)
        * `e2e_receiver` -> For End-To-End only on core B (receive)
        * `ingest_only` -> Execute basic sanity only ingestion on core A
        * `watcher` -> Discrete Agent CLI

## Environment Variables Specification and Description:
|  Variable   | Value       | Mandatory   |   Default   |
| :----------- | :-----------: | :-----------: | :-----------: |
| PYTEST_RUNNING_MODE | Described on docker mode spec | Only for docker run | - | 
| CONF_FILE | directory to config.json | + | - | 
| AGENT_URL | Route API | Only for watch cli mode | - |

## Config -> configuration.json:
##### The project repo include skeleton of the configuration with fields to fill in
* If running Full core A to core B you must fill all keys on json
* For only core A or Core B fill just the relevant keys
* The repo include sample skeleton for config.json named "config_sample.json"