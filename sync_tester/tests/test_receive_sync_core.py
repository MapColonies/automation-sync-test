"""
This pytest module include e2e test of second part of core's synchronization -> receive tiles and create layer:
* From core A
* GW
"""
import logging
import os
import time
from datetime import datetime

from conftest import *
from sync_tester.configuration import config
from sync_tester.functions import executors

is_logger_init = False
_log = logging.getLogger('sync_tester.tests.test_receive_sync_core')
s3_credential = None
storage_provider = None


def test_receive_from_gw():
    """This test validate core's process of receive tiles and generate layer on core"""
    receive_product_id = config.PRODUCT_ID_B
    receive_product_version = config.PRODUCT_VERSION_B
    assert (
            receive_product_id and receive_product_version), f"Test: [{test_receive_from_gw.__name__}] Failed: Validation layer " \
                                                             f"details\n" \
                                                             f"related errors:\n" \
                                                             f"at least on of layer params missing: product_id: [{receive_product_id}], " \
                                                             f"product_version: [{receive_product_version}]"
    _log.info(
        f'\n=======================================  Sync receiver coreB testing ========================================\n')

    # ======================================= Sync-received job task creation ==========================================

    try:

        criteria_params = {
            'timeout': config.RECEIVE_JOB_FIND_TIMEOUT_B,
            'product_id': receive_product_id,
            'product_version': receive_product_version,
            'job_type': config.JobTaskTypes.SYNC_TARGET.value,
            'job_manager_url': config.JOB_MANAGER_ROUTE_CORE_B

        }
        resp = executors.creation_job_loop_follower(criteria_params)
        # resp = executors.validate_sync_job_creation(receive_product_id,
        #                                             receive_product_version,
        #                                             config.JobTaskTypes.SYNC_TARGET.value,
        #                                             job_manager_url=config.JOB_MANAGER_ROUTE_CORE_B)
        msg = resp['message']
        sync_receive_job_state = resp['state']
        sync_receive_job = resp['record']

    except Exception as e:
        sync_receive_job_state = False
        msg = str(e)

    assert sync_receive_job_state, f'Test: [{test_receive_from_gw.__name__}] Failed: Query for new receive sync job\n' \
                                   f'related errors:\n' \
                                   f'{msg}'

    # ======================================= Sync receive job task follower ===========================================

    sync_receive_job = sync_receive_job[0]
    sync_receive_job_id = sync_receive_job['id']
    sync_receive_job_metadata = sync_receive_job['parameters']
    # cleanup_data['sync_job_id'] = sync_job_id # todo -> implement cleanup

    try:
        resp = executors.follow_sync_job(product_id=receive_product_id,
                                         product_version=receive_product_version,
                                         product_type=config.JobTaskTypes.SYNC_TARGET.value,
                                         job_manager_url=config.JOB_MANAGER_ROUTE_CORE_B,
                                         running_timeout=config.SYNC_TIMEOUT_B,
                                         internal_timeout=config.BUFFER_TIMEOUT_CORE_B)

        sync_receive_follow_state = resp['status']
        msg = resp['message']
    except Exception as e:
        sync_receive_follow_state = False
        msg = str(e)
    assert sync_receive_follow_state, f'Test: [{test_receive_from_gw.__name__}]\n' \
                                      f'Failed: Follow for sync receive job complete\n' \
                                      f'related errors:\n' \
                                      f'{msg}'

    # ======================================== Sync receive tiles validator ============================================
    tile_count_on_toc = sync_receive_job['tasks'][0]['parameters']['expectedTilesCount']
    try:
        tile_count_on_storage = executors.count_tiles_amount(receive_product_id, receive_product_version, core="B")
        tile_count_state = tile_count_on_toc == tile_count_on_storage
        msg = f'Tile count on toc: [{tile_count_on_toc}] | Tile count on Storage: [{tile_count_on_storage}]'
        _log.info(f'Tile count on toc: [{tile_count_on_toc}] | Tile count on Storage: [{tile_count_on_storage}]')
    except Exception as e:
        tile_count_state = False
        msg = str(e)

    assert tile_count_state, f'Test: [{test_receive_from_gw.__name__}] Failed: tile count validation\n' \
                             f'related errors:\n' \
                             f'{msg}'

    # =========================================== validate pycsw record ================================================
    time.sleep(config.BUFFER_TIMEOUT_CORE_B)

    try:
        validation_dict, pycsw_records, links = executors.validate_metadata_pycsw(sync_receive_job_metadata,
                                                                                  receive_product_id,
                                                                                  receive_product_version,
                                                                                  config.PYCSW_URL_B,
                                                                                  config.PYCSW_GET_RASTER_RECORD_PARAMS_B)
        pycsw_validation_state = validation_dict['validation']
        msg = validation_dict['reason']
    except Exception as e:
        pycsw_validation_state = False
        msg = str(e)

    assert pycsw_validation_state, f'Test: [{test_receive_from_gw.__name__}] Failed: Validation of toc with pycsw\n' \
                                   f'related errors:\n' \
                                   f'{msg}'

    # ========================================== validate mapproxy layer================================================
    time.sleep(config.BUFFER_TIMEOUT_CORE_B)
    try:
        params = {'mapproxy_endpoint_url': config.MAPPROXY_ROUTE_CORE_B,
                  'tiles_storage_provide': config.TILES_PROVIDER_B,
                  'grid_origin': config.MAPPROXY_GRID_ORIGIN_B,
                  'nfs_tiles_url': config.NFS_TILES_DIR_B}

        if config.TILES_PROVIDER_B.lower() == "s3":
            params['endpoint_url'] = config.S3_ENDPOINT_URL_CORE_B
            params['access_key'] = config.S3_ACCESS_KEY_CORE_B
            params['secret_key'] = config.S3_SECRET_KEY_CORE_B
            params['bucket_name'] = config.S3_BUCKET_NAME_CORE_B

        result = executors.validate_mapproxy_layer(pycsw_records, receive_product_id, receive_product_version, params)
        mapproxy_validation_state = result['validation']
        msg = result['reason']

    except Exception as e:
        mapproxy_validation_state = False
        msg = str(e)

    assert mapproxy_validation_state, f'Test: [{test_receive_from_gw.__name__}] Failed: Validation of mapproxy urls\n' \
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

    test_receive_from_gw()

_log.info('Loading tests suite for receive sync services')
