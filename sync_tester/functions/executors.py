"""
This module implement flow execution and multiple complex flow with main infrastructure layer
"""

import logging
import os
from sync_tester.configuration import config
from sync_tester.functions import discrete_ingestion_executors, data_executors
from mc_automation_tools import common
from mc_automation_tools.ingestion_api import job_manager_api
from conftest import ValueStorage

_log = logging.getLogger('sync_tester.functions.executors')


def run_ingestion():
    """
    This is preprocess that will run and create new unique layer to process sync step over
    :return: dict -> {product_id:str, product_version:str}
    """
    ingestion_data = {}
    _log.info('\n\n********************************* Start preparing for ingestion **************************************')
    _log.info('Send request to stop agent watch')
    watch_status = discrete_ingestion_executors.stop_agent_watch()  # validate not agent not watching for ingestion
    if not watch_status['state']:
        raise Exception('Failed on stop agent watch')
    _log.info(f'Stop agent watch: [message from service: {watch_status["reason"]}]')
    _log.info(f'Prepare source data of ingestion:')
    _log.info(f'Start copy source discrete data to test destination replica data:\n'
              f'Running environment: {config.ENV_NAME}\n'
              f'Storage adapter: {config.STORAGE_ADAPTER}\n'
              f'Source Dir [for NFS mode only]: {os.path.join(config.DISCRETE_RAW_ROOT_DIR, config.DISCRETE_RAW_SRC_DIR)}\n'
              f'Destination Dir [for NFS mode only]: {os.path.join(config.DISCRETE_RAW_ROOT_DIR, config.DISCRETE_RAW_DST_DIR)}')
    # ======================================== Ingestion data prepare ==================================================
    data_manager = data_executors.DataManager(config.ENV_NAME, watch=False)
    res = data_manager.init_ingestion_src()
    ingestion_data['product_id'], ingestion_data['product_version'] = res['resource_name'].split('-')
    ingestion_dir = res['ingestion_dir']
    ValueStorage.discrete_list.append(ingestion_data)  # store running data for future cleanup
    _log.info(f'\nFinish prepare of ingestion data:\n'
              f'Source on dir: {res["ingestion_dir"]}\n'
              f'SourceId: {res["resource_name"]}\n'
              f'------------------------------------- End of ingestion data preparation ----------------------------------\n')
    # ============================================= Run ingestion ======================================================
    _log.info('\n********************************* Start Discrete ingestion ***************************************** ')
    _log.info(f'Run data validation on source data')
    state, json_data = data_manager.validate_source_directory()
    if not state:
        _log.error(f'Failed on validation: status [{state}], with reason: [{json_data}]')
        raise Exception(f'Validation of source data failed: status [{state}], with reason: [{json_data}]')
    _log.info(f'Validation of data passed')
    _log.info('Send manual ingestion request')
    status, content = discrete_ingestion_executors.send_agent_manual_ingest(ingestion_dir)
    if not status == config.ResponseCode.Ok.value:
        raise Exception(f'Failed on sending manual ingestion with error: {status} and message: {content}')
    _log.info(f'Success sent new ingestion request on manual agent: [{status}]:[{content}]')
    # ============================================ Follow ingestion ====================================================
    _log.info(f'Follow ingestion running job')
    job_tasks = job_manager_api.JobsTasksManager(config.JOB_MANAGER_ROUTE)
    res = job_tasks.follow_running_job_manager(product_id=ingestion_data['product_id'],
                                               product_version=ingestion_data['product_version'],
                                               product_type=config.JOB_MANAGER_TYPE['discrete_tiling'],
                                               timeout=config.INGESTION_TIMEOUT,
                                               internal_timeout=config.BUFFER_TIMEOUT)
    _log.info(f'\n------------------------------ Discrete ingestion complete -------------------------------------------')
    # ========================================= start sync from core A =================================================


def trigger_orthphoto_history_sync(product_id, product_version):
    """

    :param product_id: str -> resource id of the layer to sync
    :param product_version: version of discrete
    :return:
    """
