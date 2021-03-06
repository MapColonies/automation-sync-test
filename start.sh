#!/bin/bash
source /source_code/venv/bin/activate
#pytest --show-capture=no /source_code/sync_tester/tests/test_trigger_sync_core.py

# shellcheck disable=SC2236
if [[ ! -z "${PYTEST_RUNNING_MODE}" ]]; then
echo -ne "Test chosen running mode is: [${PYTEST_RUNNING_MODE}]\n"

case $PYTEST_RUNNING_MODE in

  e2e)
    echo -ne " ***** Will Run End - To - End test |Azure Core A to Core B| ***** \n"
    pytest --show-capture=no /source_code/sync_tester/tests/test_sync_e2e_azure.py
    ;;

  e2e_sender)
    echo -ne " ***** Will Run End - To - End test |only sender core| ***** \n"
    pytest --show-capture=no /source_code/sync_tester/tests/test_trigger_sync_core.py
    ;;

  e2e_receiver)
    echo -ne " ***** Will Run End - To - End test |only receiver core| ***** \n"
    pytest --show-capture=no /source_code/sync_tester/tests/test_receive_sync_core.py
    ;;

  e2e_receiver_autonomic)
    echo -ne " ***** Will Run End - To - End test |only receiver with autonomic upload core| ***** \n"
    pytest --show-capture=no /source_code/sync_tester/tests/test_autonomic_sync_receive.py
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

  watcher)
  echo -ne " ***** Will Run watcher cli *****\n"
  python /source_code/sync_tester/watch_cli/watch_app.py
  ;;

  *)
    echo -ne " ----- unknown tests mode params: [PYTEST_RUNNING_MODE=$PYTEST_RUNNING_MODE] ----- \n"
    ;;
esac

else
echo "no variable provided for PYTEST_RUNNING_MODE"
fi
