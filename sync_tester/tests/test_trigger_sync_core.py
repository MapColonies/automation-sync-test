"""
This pytest module include e2e test of first part of core's synchronization -> transfer tiles and toc to other edge:
* To core B
* GW
"""
import logging

from conftest import ValueStorage
from sync_tester.configuration import config
from sync_tester.functions import executors
from sync_tester.functions import discrete_ingestion_executors

_log = logging.getLogger('sync_tester.tests.test_trigger_sync_core')


def test_trigger_to_gw():
    """This test validate core's process of trigger and send sync data out of core"""

    # ======================== prepare data (step 1) -> ingestion some discrete to core ================================
    _log.info(f'Start preprocess of sync A -> ingest of new discrete')
    try:
        ingest_res = executors.run_ingestion()
        ingestion_state = ingest_res['state']
        ingestion_product_id = ingest_res['product_id']
        ingestion_product_version = ingest_res['product_version']
        cleanup_data = ingest_res['cleanup_data']
        ValueStorage.discrete_list.append(cleanup_data)
        msg = ingest_res['message']
        _log.info(f'Ingestion complete')
    except Exception as e:
        _log.error(f'Failed on running ingestion with error: [{str(e)}]')
        ingestion_state = False
        msg = str(e)

    assert ingestion_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: New discrete failed\n' \
                            f'related errors:\n' \
                            f'{msg}'

    tiles_count = executors.count_tiles_amount(ingestion_product_id, ingestion_product_version)

    # ======================================= trigger sync by nifi api =================================================
    try:
        resp = executors.trigger_orthphoto_history_sync(ingestion_product_id, ingestion_product_version)
        trigger_sync_state, msg = resp['state'], resp['msg']
    except Exception as e:
        trigger_sync_state = False
        msg = str(e)

    assert trigger_sync_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: Send start sync trigger stage\n' \
                               f'related errors:\n' \
                               f'{msg}'

    # ======================================== Sync job task creation ==================================================

    try:
        resp = executors.validate_sync_job_creation(ingestion_product_id, ingestion_product_version, config.JobTypes.SYNC_TRIGGER.value)
        msg = resp['message']
        sync_job_state = resp['state']
        sync_job = resp['record']

    except Exception as e:
        sync_job_state = False
        msg = str(e)

    assert sync_job_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: Query for new sync job\n' \
                               f'related errors:\n' \
                               f'{msg}'

def teardown_module(module):  # pylint: disable=unused-argument
    """
    This method been executed after test running - env cleaning
    """
    # todo - future to be implemented after integration with danny
    res = executors.clean_env(ValueStorage.discrete_list)


test_trigger_to_gw()
