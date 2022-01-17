"""
This module implement flow execution and multiple complex flow with main infrastructure layer
"""

import logging
import os

from discrete_kit.functions.shape_functions import ShapeToJSON

from sync_tester.configuration import config
from sync_tester.nifi_sync_api import nifi_sync
from sync_tester.functions import discrete_ingestion_executors, data_executors
from sync_tester.postgres import postgres_adapter
from mc_automation_tools.validators import pycsw_validator, mapproxy_validator
from mc_automation_tools.ingestion_api import job_manager_api
from mc_automation_tools.sync_api import layer_spec_api
from mc_automation_tools.models import structs
from conftest import ValueStorage

_log = logging.getLogger('sync_tester.functions.executors')


def run_ingestion():
    """
    This is preprocess that will run and create new unique layer to process sync step over
    :return: dict -> {product_id:str, product_version:str}
    """
    pg_credential = config.PGProvider(config.PG_ENDPOINT_URL_CORE_A,
                                      config.PG_USER_CORE_A,
                                      config.PG_PASS_CORE_A,
                                      config.PG_JOB_TASK_DB_CORE_A,
                                      config.PG_PYCSW_RECORD_DB_CORE_A,
                                      config.PG_MAPPROXY_DB_CORE_A,
                                      config.PG_AGENT_DB_CORE_A)

    pg_handler = postgres_adapter.PostgresHandler(pg_credential)
    initial_mapproxy_configs = pg_handler.get_mapproxy_configs()
    ingestion_data = {}
    _log.info(
        '\n\n*********************************** Start preparing for ingestion **************************************')

    _log.info('Send request to stop agent watch')
    if config.SOURCE_DATA_PROVIDER_A.lower() == "pv" or config.SOURCE_DATA_PROVIDER_A.lower() == "pvc":
        discrete_raw_root_dir = config.DISCRETE_RAW_ROOT_DIR_CORE_A
    elif config.SOURCE_DATA_PROVIDER_A.lower() == "nfs" or config.SOURCE_DATA_PROVIDER_A.lower() == "fs":
        discrete_raw_root_dir = config.NFS_RAW_DST_DIR_CORE_A
    discrete_agent_adapter = discrete_ingestion_executors.DiscreteAgentAdapter(entrypoint_url=config.DISCRETE_JOB_MANAGER_URL_CORE_A,
                                                                               source_data_provider=config.SOURCE_DATA_PROVIDER_A,
                                                                               discrete_raw_root_dir=discrete_raw_root_dir)
    watch_status = discrete_agent_adapter.stop_agent_watch()  # validate not agent not watching for ingestion
    if not watch_status['state']:
        raise Exception('Failed on stop agent watch')
    _log.info(f'Stop agent watch: [message from service: {watch_status["reason"]}]')
    _log.info(f'Prepare source data of ingestion:')
    _log.info(f'Start copy source discrete data to test destination replica data:\n'
              f'Running environment: {config.ENV_NAME}\n'
              f'Storage adapter: {config.SOURCE_DATA_PROVIDER_A}\n'
              f'Source Dir [for NFS mode only]: {os.path.join(config.DISCRETE_RAW_ROOT_DIR_CORE_A, config.DISCRETE_RAW_SRC_DIR_CORE_A)}\n'
              f'Destination Dir [for NFS mode only]: {os.path.join(config.DISCRETE_RAW_ROOT_DIR_CORE_A, config.DISCRETE_RAW_DST_DIR_CORE_A)}')

    # ======================================== Ingestion data prepare ==================================================
    if config.TILES_PROVIDER_A.lower() == "s3":
        s3_credential = config.S3Provider(entrypoint_url=config.S3_ENDPOINT_URL_CORE_A,
                                          access_key=config.S3_ACCESS_KEY_CORE_A,
                                          secret_key=config.S3_SECRET_KEY_CORE_A,
                                          bucket_name=config.S3_BUCKET_NAME_CORE_A)
    else:
        s3_credential = None

    storage_provider = config.StorageProvider(source_data_provider=config.SOURCE_DATA_PROVIDER_A,
                                              tiles_provider=config.TILES_PROVIDER_A,
                                              s3_credential=s3_credential,
                                              pvc_handler_url=config.PVC_HANDLER_URL_CORE_A)

    data_manager = data_executors.DataManager(watch=False,
                                              storage_provider=storage_provider,
                                              update_zoom=config.UPDATE_ZOOM_CORE_A,
                                              zoom_level_change=config.MAX_ZOOM_LEVEL_CORE_A
                                              )
    res = data_manager.init_ingestion_src()
    ingestion_data['product_id'], ingestion_data['product_version'] = res['resource_name'].split('-')
    ingestion_dir = res['ingestion_dir']
    ValueStorage.discrete_list.append(ingestion_data)  # store running data for future cleanup
    _log.info(f'\nFinish prepare of ingestion data:\n'
              f'Source on dir: {res["ingestion_dir"]}\n'
              f'SourceId: {res["resource_name"]}\n'
              f'----------------------------------- End of ingestion data preparation ---------------------------------\n')

    # =============================================== Run ingestion ========================================================
    _log.info('\n************************************ Start Discrete ingestion *****************************************')
    _log.info(f'Run data validation on source data')
    state, json_data = data_manager.validate_source_directory()
    if not state:
        _log.error(f'Failed on validation: status [{state}], with reason: [{json_data}]')
        raise Exception(f'Validation of source data failed: status [{state}], with reason: [{json_data}]')
    _log.info(f'Validation of data passed')
    _log.info('Send manual ingestion request')

    status, content = discrete_agent_adapter.send_agent_manual_ingest(ingestion_dir)
    if not status == config.ResponseCode.Ok.value:
        raise Exception(f'Failed on sending manual ingestion with error: {status} and message: {content}')
    _log.info(f'Success sent new ingestion request on manual agent: [{status}]:[{content}]')

    # ============================================= Follow ingestion ===================================================
    _log.info(f'Follow ingestion running job')
    job_tasks = job_manager_api.JobsTasksManager(config.JOB_MANAGER_ROUTE_CORE_A)
    res = job_tasks.follow_running_job_manager(product_id=ingestion_data['product_id'],
                                               product_version=ingestion_data['product_version'],
                                               product_type=config.JobTaskTypes.DISCRETE_TILING.value,
                                               timeout=config.INGESTION_TIMEOUT_CORE_A,
                                               internal_timeout=config.BUFFER_TIMEOUT_CORE_A)

    if sum(1 for task in res['tasks'] if task['attempts'] > 0):
        _log.warning(f'Some of related tasks were found with "attempts" > 0')
        _log.debug(f'Related Tasks with attempts > 0 are:\n')
        for task in res['tasks']:
            if task['attempts'] > 0:
                _log.debug(f'Task ID: [{task["id"]}], No. Attempts: [{task["attempts"]}]')
    _log.info(
        f'\n------------------------------ Discrete ingestion complete -------------------------------------------\n')
    cleanup_data = {
        'product_id': ingestion_data['product_id'],
        'product_version': ingestion_data['product_version'],
        "mapproxy_last_id": initial_mapproxy_configs[0]['id'],
        "mapproxy_length": len(initial_mapproxy_configs),
        "folder_to_delete": os.path.join(config.DISCRETE_RAW_ROOT_DIR_CORE_A, config.DISCRETE_RAW_DST_DIR_CORE_A),
        "tiles_folder_to_delete": "tiles",
        "watch_status": False,
        "volume_mode": "nfs"
    }
    if res['status'] == 'Completed':
        ingestion_state = True
        job_id = res['job_id']
        msg = res['message']
    else:
        ingestion_state = False
        job_id = None
        msg = res['message']

    return {'state': ingestion_state,
            'product_id': ingestion_data['product_id'],
            'product_version': ingestion_data['product_version'],
            'cleanup_data': cleanup_data,
            'job_id': job_id,
            'message': msg}


# =========================================== start sync from core A ===================================================


def count_tiles_amount(product_id, product_version, core):
    """

    :param product_id: str -> resource id of the layer to sync
    :param product_version: version of discrete
    :param core: "A" [send] | "B" [received]
    :return: int -> total amount of tiles
    """

    if core.lower() == "b":
        if config.TILES_PROVIDER_B:
            s3_credential = config.S3Provider(entrypoint_url=config.S3_ENDPOINT_URL_CORE_B,
                                              access_key=config.S3_ACCESS_KEY_CORE_B,
                                              secret_key=config.S3_SECRET_KEY_CORE_B,
                                              bucket_name=config.S3_BUCKET_NAME_CORE_B)
        else:
            s3_credential = None
        storage_provider = config.StorageProvider(tiles_provider=config.TILES_PROVIDER_B,
                                                  s3_credential=s3_credential)
    elif core.lower() == "a":
        if config.TILES_PROVIDER_A:
            s3_credential = config.S3Provider(entrypoint_url=config.S3_ENDPOINT_URL_CORE_A,
                                              access_key=config.S3_ACCESS_KEY_CORE_A,
                                              secret_key=config.S3_SECRET_KEY_CORE_A,
                                              bucket_name=config.S3_BUCKET_NAME_CORE_A)
        else:
            s3_credential = None

        storage_provider = config.StorageProvider(tiles_provider=config.TILES_PROVIDER_A,
                                                  s3_credential=s3_credential)

    data_manager = data_executors.DataManager(watch=False, storage_provider=storage_provider)
    res = data_manager.count_tiles_on_storage(product_id, product_version)
    return res


def trigger_orthphoto_history_sync(product_id, product_version):
    """
    This method will trigger new sync job for OrthophotoHistory type
    :param product_id: str -> resource id of the layer to sync
    :param product_version: version of discrete
    :return:
    """
    _log.info(
        '\n\n******************************** Start Triggering Sync for ingestion *************************************')

    sync_request_body = {
        'resourceId': product_id,
        'version': product_version,
        'operation': config.SyncOperationType.ADD.value,
        'productType': config.ProductType.orthophoto_history.value,
        'layerRelativePath': os.path.join(product_id, product_version, config.ProductType.orthophoto_history.value)
    }
    resp = nifi_sync.send_sync_request(sync_request_body)
    s_code = resp['status_code']
    msg = resp['msg']['message']

    if s_code == config.ResponseCode.Ok.value:
        _log.info(f'Request sync success:\n'
                  f'Status code: [{s_code}]\n'
                  f'Message: [{msg}]')
        state = True
    else:
        state = False
        _log.error(f'Request sync was failed:\n'
                   f'Status code: [{s_code}]\n'
                   f'Message: [{msg}]')
    # state = True if resp['status_code'] == config.ResponseCode.Ok.value else False
    msg = f"status code: [{s_code}] | message: {msg}"

    _log.info(
        f'\n----------------------------------- Finish Triggering Sync -----------------------------------------------')
    return {"state": state, "msg": msg}


# ============================================ job manager controllers ============-====================================


def validate_sync_job_creation(product_id, product_version, job_type, job_manager_url):
    """
    This method query by job manager api and find if sync job is exists
    :param product_id: str -> resource id of the layer to sync
    :param product_version: version of discrete
    :param job_type: str -> [SYNC_TRIGGER]
    :param job_manager_url: str -> url of job manager
    :return: dict -> {state: bool, message: str, records: list[dict]}
    """
    params = {
        'resourceId': product_id,
        'version': product_version,
        'type': job_type
    }

    res = {
        'state': True,
        'message': 'OK',
        'record': None}

    _log.info(f' Will query for sync job with parameters:\n'
              f'{params}')
    job_manager_client = job_manager_api.JobsTasksManager(job_manager_url)
    try:
        resp = job_manager_client.find_jobs_by_criteria(params)
        if not len(resp):
            res['state'] = False
            res['message'] = 'No records found'
            _log.info(f'Sync job not found current sync request: [{params}]')
            return res

        _log.info(f'Found {len(resp)} jobs for current sync request')
        res['record'] = resp
        return res

    except Exception as e:
        res['state'] = False
        res['message'] = f'Failed find job with error: {str(e)}'
        return res


def follow_sync_job(product_id, product_version, product_type, job_manager_url, running_timeout=300, internal_timeout=80):
    """
    This method will follow job and task of sync
    :param product_id: layer's resource id
    :param product_version: layer's version
    :param product_type: job type
    :param job_manager_url: route url for job manager api
    :param running_timeout: timeout to abort following in case of sync deadlock
    :param internal_timeout: internal timeout to prevent system crashes
    :return: dict -> {state: bool, msg: str}
    """

    _log.info(
        '\n\n************************************ Start Follow Sync job ***********************************************')
    _log.debug(f'Parameters for follow sync job:\n'
               f'Product ID: {product_id}\n'
               f'Product version: {product_id}\n'
               f'Product Type: {product_type}\n'
               f'Follow timeout bounds: {running_timeout} sec\n'
               f'Internal system delay {internal_timeout}')

    job_manager_client = job_manager_api.JobsTasksManager(job_manager_url)
    res = job_manager_client.follow_running_job_manager(product_id=product_id,
                                                        product_version=product_version,
                                                        product_type=product_type,
                                                        timeout=running_timeout,
                                                        internal_timeout=internal_timeout)
    _log.debug(f'Results from following sync job:\n'
               f'State: {res["status"]}\n'
               f'Message: {res["message"]}')

    if sum(1 for task in res['tasks'] if task['attempts'] > 0):
        _log.warning(f'Some of related tasks were found with "attempts" > 0')
        _log.debug(f'Related Tasks with attempts > 0 are:\n')
        for task in res['tasks']:
            if task['attempts'] > 0:
                _log.debug(f'Task ID: [{task["id"]}], No. Attempts: [{task["attempts"]}]')

    _log.info(
        f'\n----------------------------------- Finish Follow Sync -----------------------------------------------')
    return res


# ============================================= core A validation ======================================================


def get_layer_spec_tile_count(layer_id, target, layer_spec_api):
    """
    This method query layer spec api and receive tile count as written after core A sync process and compare with
    expected value that provided.
    :param layer_id: "product_id-product_version"
    :param target: destination target
    :param layer_spec_api:url for layer spec route
    :return: dict -> {state:bool, msg:str}
    """
    _log.info(
        '\n\n****************************** Get actual tile count on layer spec ***************************************')
    try:
        layer_spec = layer_spec_api.LayerSpec(layer_spec_api)
        status_code, res = layer_spec.get_tiles_count(layer_id=layer_id,
                                                      target=target)

        _log.info(f'Request tile count on layer spec\n'
                  f'Status code: {status_code}\n'
                  f'Tiles count: {res}\n')
        if status_code != config.ResponseCode.Ok.value:
            res = {'state': False, 'message': f'Failed with error code: {status_code} and error message: [{res}]'}

        else:
            result = res.get('tilesCount')
            if result > 0:
                res = {'state': True, 'message': f'Layer spec include tiles count value: {result}', 'tile_count': result}
            else:
                res = {'state': False, 'message': f'Layer spec [{result}] are not > 0', 'tile_count': result}

    except Exception as e:
        res = {'state': False, 'message': f'Failed layer spec validation with error: [{str(e)}]', 'tile_count': None}

    _log.info(
        f'\n----------------------------------- Finish layer spec tile count -------------------------------------------')

    return res

def validate_toc_task_creation(job_id, expected_tiles_count, toc_job_type=config.JobTaskTypes.TOC_SYNC.value):
    """
    The method validate core A toc task creation and validate num of tiles
    :param job_id: id of related job for toc task
    :param expected_tiles_count: original tile amount that was created on ingestion
    :param toc_job_type: The type as represented for toc type task
    :return: dict -> {'state': bool, 'reason': 'str, 'toc': dict}
    """
    param = {
        "jobId": job_id,
        "type": toc_job_type
    }
    _log.info(f'\n********************************* Start toc Sync validation ******************************************')
    _log.info(f'\nPrepare validation of tile count on toc for:\n'
              f'{param}\n'
              f'Expected tiles: {expected_tiles_count}')
    result = {}
    try:
        job_manager_client = job_manager_api.JobsTasksManager(config.JOB_MANAGER_ROUTE_CORE_A)
        resp = job_manager_client.find_tasks(param)[0]
        toc = resp['parameters']['tocData']
        result['state'] = toc['tileCount'] == expected_tiles_count
        result['reason'] = f'Expected tiles count: [{expected_tiles_count}] | actual from toc: [{toc["tileCount"]}]'
        result['toc'] = toc
    except Exception as e:
        _log.error(f'failure on getting toc task: {str(e)}')
        result['state'] = False
        result['reason'] = str(e)
        result['toc'] = None

    _log.info(f'\nValidation for toc complete with results:\n'
              f'state: {result["state"]}\n'
              f'message: {result["reason"]}\n')

    _log.info(f'\n\n-------------------------------- Finish toc Sync validation -----------------------------------------')
    return result

# ============================================== pycsw controllers =====================================================


# def get_records_by_layer(layer_id, layer_version, pycsw_url, query_params):
#     """
#
#     :param layer_id: id [str]
#     :param layer_version: version [str]
#     :param query_params: get params for csw [dict]:
#         {
#             "service": "CSW",
#             "version": "2.0.2",
#             "request": "GetRecords",
#             "typenames": "mc:MCRasterRecord",
#             "ElementSetName": "full",
#             "resultType": "results",
#             "outputSchema": "http://schema.mapcolonies.com/raster"
#         }
#     :return: dict -> record data
#     """
#     pycsw_conn = pycsw_validator.PycswHandler(pycsw_url, query_params)


def validate_metadata_pycsw(metadata, layer_id, layer_version, pycsw_url, query_params):
    """
    The method validate computability between metadata written to toc against the actual data on csw's records
    :param metadata: source metadata provided as dict -> {metadata: {...}}
    :param layer_id: id represent the layer [str]
    :param layer_version: version of the layer [str]
    :param pycsw_url: route to csw server
    :param query_params:
     ** example:
                {
                    'service': PYCSW_SERVICE, ["CSW"]
                    'version': PYCSW_VERSION, ["2.0.2"]
                    'request': PYCSW_REQUEST_GET_RECORDS, ["GetRecords"]
                    'typenames': PYCSW_TYPE_NAMES, ["mc:MCRasterRecord"]
                    'ElementSetName': PYCSW_ElEMENT_SET_NAME, ["full"]
                    'outputFormat': PYCSW_OUTPUT_FORMAT, ["application/xml"]
                    'resultType': PYCSW_RESULT_TYPE, ["results"]
                    'outputSchema': PYCSW_OUTPUT_SCHEMA [None]
                }

    :return: result dict -> {'validation': bool, 'reason':{}}, pycsw_records -> dict, links -> dict
    """
    try:
        _log.info(f'\n\n******************** Will run validation of toc metadata vs. pycsw record ************************')
        pycsw_conn = pycsw_validator.PycswHandler(pycsw_url, query_params)
        toc_json = {'metadata': ShapeToJSON().create_metadata_from_toc(metadata['metadata'])}
        results = pycsw_conn.validate_pycsw(toc_json, layer_id, layer_version)
        res_dict = results['results']
        pycsw_records = results['pycsw_record']
        links = results['links']

    except Exception as e:
        _log.error(f'Failed validation of pycsw with error: [{str(e)}]')
        res_dict = {'validation': False, 'reason': str(e)}
        pycsw_records = {}
        links = {}

    _log.info(f'\n----------------------- Finish validation of toc metadata vs. pycsw record --------------------------')

    return res_dict, pycsw_records, links

# ============================================= mapproxy controllers ===================================================


def validate_mapproxy_layer(pycsw_record, product_id, product_version, params=None):
    """
    This method will ensure the url's provided on mapproxy from pycsw
    :return: result dict -> {'validation': bool, 'reason':{}}, links -> dict
    """
    _log.info(f'\n\n******************** Will run validation of layer mapproxy vs. pycsw record ************************')

    if params['tiles_storage_provide'].lower() == 's3':
        s3_credential = structs.S3Provider(entrypoint_url=params['endpoint_url'],
                                           access_key=params['access_key'],
                                           secret_key=params['secret_key'],
                                           bucket_name=params['bucket_name'])
    else:
        s3_credential = None
    mapproxy_conn = mapproxy_validator.MapproxyHandler(entrypoint_url=params['mapproxy_endpoint_url'],
                                                       tiles_storage_provide=params['tiles_storage_provide'],
                                                       grid_origin=params['grid_origin'],
                                                       s3_credential=s3_credential,
                                                       nfs_tiles_url=params['nfs_tiles_url'])

    res = mapproxy_conn.validate_layer_from_pycsw(pycsw_record, product_id, product_version)
    return res

    _log.info(f'\n----------------------- Finish validation of layer mapproxy vs. pycsw record --------------------------')
# ================================================== cleanup ===========================================================


# todo -> need implantation and integration with automation cleanup package
def clean_env(delete_request):
    """
    This method will cleanup running environment after test running
    :param delete_request: json with params, example:
        {
          "product_id": "2021_12_14T13_10_45Z_MAS_6_ORT_247557",
          "product_version": "4.0",
          "mapproxy_last_id": 1,
          "mapproxy_length": 1,
          "folder_to_delete": "/tmp/mid_dir/watch",
          "tiles_folder_to_delete": "adsa",
          "watch_state": "False",
          "volume_mode": "nfs"
        }
    :return: result dict
    """
    _log.debug(f'Current data parameters for deletion process:\n'
               f'{delete_request}')

    if config.ENV_NAME == config.EnvironmentTypes.QA.name or config.ENV_NAME == config.EnvironmentTypes.DEV.name:
        print('will do cleanup for azure environment')
        return {'status': 'Need to be updated'}

    elif config.ENV_NAME == config.EnvironmentTypes.PROD.name:
        print('will do cleanup for production environment')
        return {'status': 'Need to be updated'}

    else:
        raise ValueError(f'Illegal environment value type: {config.ENV_NAME}')
