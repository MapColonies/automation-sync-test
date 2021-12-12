"""
This module implement flow execution and multiple complex flow with main infrastructure layer
"""

import logging
import os
from sync_tester.configuration import config
from sync_tester.functions import discrete_ingestion_executors, data_executors
from mc_automation_tools import common
from conftest import ValueStorage

_log = logging.getLogger('sync_tester.functions.executors')


def run_ingestion():
    """
    This is preprocess that will run and create new unique layer to process sync step over
    :return: dict -> {product_id:str, product_version:str}
    """
    ingestion_data = {}
    _log.info('\n******* Start preparing for ingestion *******')
    _log.info('Send request to stop agent watch')
    watch_status = discrete_ingestion_executors.stop_agent_watch()  # validate not agent not watching for ingestion
    if watch_status['state']:
        raise Exception('Failed on stop agent watch')
    _log.info(f'Stop agent watch: [message from service: {watch_status["reason"]}]')
    _log.info(f'Prepare source data of ingestion:')
    _log.info(f'Start copy source discrete data to test destination replica data:\n'
              f'Running environment: {config.ENV_NAME}\n'
              f'Storage adapter: {config.STORAGE_ADAPTER}\n'
              f'Source Dir [for NFS mode only]: {os.path.join(config.DISCRETE_RAW_ROOT_DIR, config.DISCRETE_RAW_SRC_DIR)}\n'
              f'Destination Dir [for NFS mode only]: {os.path.join(config.DISCRETE_RAW_ROOT_DIR, config.DISCRETE_RAW_DST_DIR)}')
    data_manager = data_executors.DataManager(config.ENV_NAME, watch=False)
    res = data_manager.init_ingestion_src()
    ingestion_data['product_id'], ingestion_data['product_version'] = res['resource_name'].split('-')
    ValueStorage.discrete_list.append(ingestion_data)  # store running data for future cleanup
    _log.info(f'\nFinish prepare of ingestion data:\n'
              f'Source on dir: {res["ingestion_dir"]}\n'
              f'SourceId: {res["resource_name"]}\n'
              f'------------------------------------- End of ingestion data preparation ----------------------------------\n')
    _log.info('\n******* Start Discrete ingestion ******* ')
    _log.info('Send manual ingestion request')








def trigger_orthphoto_history_sync(product_id, product_version):
    """

    :param product_id: str -> resource id of the layer to sync
    :param product_version: version of discrete
    :return:
    """


