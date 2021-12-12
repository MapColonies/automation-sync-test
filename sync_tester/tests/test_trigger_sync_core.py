"""
This pytest module include e2e test of first part of core's synchronization -> transfer tiles and toc to other edge:
* To core B
* GW
"""
import logging
from sync_tester.functions import executors

_log = logging.getLogger('sync_tester.tests.test_trigger_sync_core')


def test_trigger_to_gw():
    """This test validate core's process of trigger and send sync data out of core"""

    # prepare data (step 1) -> ingestion some discrete to core
    _log.info(f'Start preprocess of sync A -> ingest of new discrete')
    executors.run_ingestion()



test_trigger_to_gw()