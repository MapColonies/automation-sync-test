#!/bin/bash
source /source_code/venv/bin/activate
#pytest --show-capture=no /source_code/sync_tester/tests/test_trigger_sync_core.py

# shellcheck disable=SC2236
if [[ ! -z "${PYTEST_RUNNING_MODE}" ]]; then
echo -ne "Test chosen running mode is: [${PYTEST_RUNNING_MODE}]\n"

case $PYTEST_RUNNING_MODE in

  e2e)
    echo -ne " ***** Will Run End - To - End test ***** \n"
    pytest --show-capture=no /source_code/sync_tester/tests/test_trigger_sync_core.py
    ;;

  e2e_sender)
    echo -dockene " ***** Will Run End - To - End test ***** \n"
    pytest --show-capture=no /source_code/sync_tester/tests/test_trigger_sync_core.py
    ;;

  full)
    echo -ne " ***** Will Run full set of tests: e2e, failures, functional tests ***** \n"
    ;;

  failures)
    echo -ne "Will Run failures tests\n"
    ;;

  functional)
    echo -ne " ***** Will Run functional tests *****\n"
    ;;

  ingest_only)
    echo -ne " ***** Will Run only ingestion *****\n"
    pytest --show-capture=no /source_code/sync_tester/tests/test_run_only_ingestion.py
    ;;

  *)
    echo -ne " ----- unknown tests mode params: [PYTEST_RUNNING_MODE=$PYTEST_RUNNING_MODE] ----- \n"
    ;;
esac

else
echo "no variable provided for PYTEST_RUNNING_MODE"
fi
