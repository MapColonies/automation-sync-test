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
    try:
        ingest_res = executors.run_ingestion()
        ingestion_state = ingest_res['state']
        ingestion_product_id = ingest_res['product_id']
        ingestion_product_version = ingest_res['product_version']
        msg = True
        _log.info(f'Ingestion complete')
    except Exception as e:
        _log.error(f'Failed on running ingestion with error: [{str(e)}]')
        ingestion_state = False
        msg = str(e)

    assert ingestion_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: New discrete failed\n' \
                            f'related errors:\n' \
                            f'{msg}'

    tiles_count = executors.count_tiles_amount(ingestion_product_id, ingestion_product_version)


test_trigger_to_gw()
