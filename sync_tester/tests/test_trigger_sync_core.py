"""
This pytest module include e2e test of first part of core's synchronization -> transfer tiles and toc to other edge:
* To core B
* GW
"""
import logging
import os
from datetime import datetime

from conftest import *
from sync_tester.configuration import config
from sync_tester.functions import executors

is_logger_init = False
_log = logging.getLogger('sync_tester.tests.test_trigger_sync_core')
_log.info('Loading tests suite for sync services')


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

    tiles_count = executors.count_tiles_amount(ingestion_product_id, ingestion_product_version, core="a")

    # ======================================= trigger sync by nifi api =================================================
    if config.SYNC_FROM_A_MANUAL:
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
        resp = executors.validate_sync_job_creation(ingestion_product_id,
                                                    ingestion_product_version,
                                                    config.JobTaskTypes.SYNC_TRIGGER.value,
                                                    job_manager_url=config.JOB_MANAGER_ROUTE_CORE_A)
        msg = resp['message']
        sync_job_state = resp['state']
        sync_job = resp['record']

    except Exception as e:
        sync_job_state = False
        msg = str(e)

    assert sync_job_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: Query for new sync job\n' \
                           f'related errors:\n' \
                           f'{msg}'

    # ======================================== Sync job task follower ==================================================

    sync_job = sync_job[0]
    sync_job_id = sync_job['id']
    cleanup_data['sync_job_id'] = sync_job_id

    try:
        resp = executors.follow_sync_job(product_id=ingestion_product_id,
                                         product_version=ingestion_product_version,
                                         product_type=config.JobTaskTypes.SYNC_TRIGGER.value,
                                         job_manager_url=config.JOB_MANAGER_ROUTE_CORE_A,
                                         running_timeout=config.SYNC_TIMEOUT,
                                         internal_timeout=config.BUFFER_TIMEOUT_CORE_A)
        sync_follow_state = True if resp['status'] == config.JobStatus.Completed.value else False
        msg = resp['message']
    except Exception as e:
        sync_follow_state = False
        msg = str(e)
    assert sync_follow_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: Follow for sync job complete\n' \
                              f'related errors:\n' \
                              f'{msg}'

    # ====================================== Validate end of core A side ===============================================

    try:
        layer_id = "-".join([ingestion_product_id, ingestion_product_version])
        target = config.CORE_TARGET
        resp = executors.validate_layer_spec_tile_count(layer_id, target, tiles_count)
        layer_spec_state = resp['state']
        msg = resp['message']

    except Exception as e:
        layer_spec_state = False
        msg = str(e)
    assert layer_spec_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: Validation of tiles count on layer spec\n' \
                             f'related errors:\n' \
                             f'{msg}'

    # ====================================== Validate end of core A side ===============================================

    try:

        resp = executors.validate_toc_task_creation(sync_job_id, tiles_count, config.JobTaskTypes.TOC_SYNC.value)
        toc_count_state = resp['state']
        toc = resp['toc']
        msg = resp['reason']
    except Exception as e:
        toc_count_state = False
        msg = str(e)

    assert toc_count_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: Validation of tiles count on toc\n' \
                            f'related errors:\n' \
                            f'{msg}'

    _log.info(f'Validation of toc metadata - pycsw records metadata')

    try:
        validation_dict, pycsw_records, links = executors.validate_metadata_pycsw(toc,
                                                                                  ingestion_product_id,
                                                                                  ingestion_product_version,
                                                                                  config.PYCSW_URL_A,
                                                                                  config.PYCSW_GET_RASTER_RECORD_PARAMS_A)

        pycsw_validation_state = validation_dict['validation']
        msg = validation_dict['reason']
    except Exception as e:
        pycsw_validation_state = False
        msg = str(e)

    assert pycsw_validation_state, f'Test: [{test_trigger_to_gw.__name__}] Failed: Validation of toc with pycsw\n' \
                                   f'related errors:\n' \
                                   f'{msg}'


def setup_module(module):
    """
    base init of running tests
    """
    init_logger()


def teardown_module(module):  # pylint: disable=unused-argument
    """
    This method been executed after test running - env cleaning
    """
    # todo - future to be implemented after integration with danny
    res = executors.clean_env(ValueStorage.discrete_list)


def init_logger():
    global is_logger_init
    if is_logger_init:
        return

    else:
        is_logger_init = True

        log_mode = config.DEBUG_LOG  # Define if use debug+ mode logs -> default info+
        file_log = config.LOG_TO_FILE  # Define if write std out into file
        log_output_path = config.LOG_OUTPUT_PATH  # The directory to write log output

        # init logger
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.setLevel(logging.DEBUG)

        # define default handler to std out
        ch = logging.StreamHandler()

        # validate log level mode to define
        if not log_mode:
            ch.setLevel(logging.INFO)
        else:
            ch.setLevel(logging.DEBUG)

        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # defining another handler to file on case it is been requested
        if file_log:
            log_file_name = ".".join([str(datetime.utcnow()), 'log'])  # pylint: disable=invalid-name
            fh = logging.FileHandler(os.path.join(log_output_path, log_file_name))
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(ch)


# todo -> on prod should run from setup pytest
if config.DEBUG:
    init_logger()
    test_trigger_to_gw()
