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

#### *Local* run + deploy:
1. Download repo: [cmd prompt] `git clone https://github.com/MapColonies/automation-sync-test.git`
2. Create venv inside the project [cmd prompt] `python -m venv venv`
3. Activate the venv: [cmd prompt] `source venv/bin/activate`
4. Run installation of framework [cmd prompt] `pip install .`

