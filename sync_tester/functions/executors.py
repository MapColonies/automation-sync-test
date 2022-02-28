"""
This module implement flow execution and multiple complex flow with main infrastructure layer
"""
import json
import logging
import os
import glob
import time
from discrete_kit.functions.shape_functions import ShapeToJSON
from mc_automation_tools.sync_api.gw_file_receiver import FileReceiver

from sync_tester.configuration import config
from sync_tester.nifi_sync_api import nifi_sync
from sync_tester.functions import discrete_ingestion_executors, data_executors
from sync_tester.postgres import postgres_adapter
from mc_automation_tools.validators import pycsw_validator, mapproxy_validator
from mc_automation_tools.ingestion_api import job_manager_api
from mc_automation_tools.parse import stringy
from mc_automation_tools.sync_api import layer_spec_api
from mc_automation_tools.models import structs
from mc_automation_tools import common
from conftest import ValueStorage

_log = logging.getLogger('sync_tester.functions.executors')


def run_ingestion():
    """
    This is preprocess that will run and create new unique layer to process sync step over
    :return: dict -> {product_id:str, product_version:str}
    """
    # if config.DB_ACCESS:
    #     res_mapproxy_config = get_mapproxy_configuration({"entrypoint_url": config.PG_ENDPOINT_URL_CORE_A,
    #                                                       "port": config.PG_PORT_A,
    #                                                       "pg_user": config.PG_USER_CORE_A,
    #                                                       "pg_pass": config.PG_PASS_CORE_A,
    #                                                       "pg_job_task_db": config.PG_JOB_TASK_DB_CORE_A,
    #                                                       "pg_pycsw_db": config.PG_PYCSW_RECORD_DB_CORE_A,
    #                                                       "pg_mapproxy_db": config.PG_MAPPROXY_DB_CORE_A,
    #                                                       "pg_agent_db": config.PG_AGENT_DB_CORE_A
    #                                                       })
    #     mapproxy_last_id = res_mapproxy_config['last_id']
    #     mapproxy_length = res_mapproxy_config['length']
    #
    #     # legacy code -> remove after two test iteration + deployment
    #     # pg_credential = config.PGProvider(entrypoint_url=config.PG_ENDPOINT_URL_CORE_A,
    #     #                                   port=config.PG_PORT_A,
    #     #                                   pg_user=config.PG_USER_CORE_A,
    #     #                                   pg_pass=config.PG_PASS_CORE_A,
    #     #                                   pg_job_task_db=config.PG_JOB_TASK_DB_CORE_A,
    #     #                                   pg_pycsw_record_db=config.PG_PYCSW_RECORD_DB_CORE_A,
    #     #                                   pg_mapproxy_db=config.PG_MAPPROXY_DB_CORE_A,
    #     #                                   pg_agent_db=config.PG_AGENT_DB_CORE_A)
    #     #
    #     # pg_handler = postgres_adapter.PostgresHandler(pg_credential)
    #     # initial_mapproxy_configs = pg_handler.get_mapproxy_configs()
    #     # mapproxy_last_id = initial_mapproxy_configs[0]['id']
    #     # mapproxy_length = len(initial_mapproxy_configs)
    # else:
    #     mapproxy_last_id = None
    #     mapproxy_length = None

    ingestion_data = {}
    stringy.pad_with_stars('Start preparing for ingestion')
    _log.info(
        '\n\n' + stringy.pad_with_stars('Start preparing for ingestion'))

    _log.info('Send request to stop agent watch')

    discrete_agent_adapter = discrete_ingestion_executors.DiscreteAgentAdapter(
        entrypoint_url=config.DISCRETE_AGENT_CORE_A,
        source_data_provider=config.SOURCE_DATA_PROVIDER_A)

    watch_status = discrete_agent_adapter.stop_agent_watch()  # validate - agent not watching for ingestion
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
    if config.SOURCE_DATA_PROVIDER_A.lower() == "pv" or config.SOURCE_DATA_PROVIDER_A.lower() == "pvc":
        fs_provider = structs.FSProvider(is_fs=False,
                                         root_dir_path=config.DISCRETE_RAW_ROOT_DIR_CORE_A,
                                         src_relative_path=config.DISCRETE_RAW_SRC_DIR_CORE_A,
                                         dst_relative_path=config.DISCRETE_RAW_DST_DIR_CORE_A)
    elif config.SOURCE_DATA_PROVIDER_A.lower() == "fs" or config.SOURCE_DATA_PROVIDER_A.lower() == "nfs":
        fs_provider = structs.FSProvider(is_fs=True,
                                         root_dir_path=config.NFS_RAW_ROOT_DIR_CORE_A,
                                         src_relative_path=config.NFS_RAW_SRC_DIR_CORE_A,
                                         dst_relative_path=config.NFS_RAW_DST_DIR_CORE_A)
    else:
        raise ValueError('[SOURCE_DATA_PROVIDER_A] environ not valid provided')

    if config.TILES_PROVIDER_A.lower() == "s3":
        # s3_credential = config.S3Provider(entrypoint_url=config.S3_ENDPOINT_URL_CORE_A,
        #                                   access_key=config.S3_ACCESS_KEY_CORE_A,
        #                                   secret_key=config.S3_SECRET_KEY_CORE_A,
        #                                   bucket_name=config.S3_BUCKET_NAME_CORE_A)
        s3_credential = structs.S3Provider(entrypoint_url=config.S3_ENDPOINT_URL_CORE_A,
                                           access_key=config.S3_ACCESS_KEY_CORE_A,
                                           secret_key=config.S3_BUCKET_NAME_CORE_A,
                                           bucket_name=config.S3_BUCKET_NAME_CORE_A)
    else:
        s3_credential = None
    # storage_provider = config.StorageProvider(source_data_provider=config.SOURCE_DATA_PROVIDER_A,
    #                                           tiles_provider=config.TILES_PROVIDER_A,
    #                                           s3_credential=s3_credential,
    #                                           pvc_handler_url=config.PVC_HANDLER_URL_CORE_A)
    storage_provider = structs.StorageProvider(source_data_provider=config.SOURCE_DATA_PROVIDER_A,
                                               tiles_provider=config.TILES_PROVIDER_A,
                                               s3_credential=s3_credential,
                                               pvc_handler_url=config.PVC_HANDLER_URL_CORE_A,
                                               fs_provider=fs_provider)

    data_manager = data_executors.DataManager(watch=False,
                                              storage_provider=storage_provider,
                                              update_zoom=config.UPDATE_ZOOM_CORE_A,
                                              zoom_level_change=config.MAX_ZOOM_LEVEL_CORE_A
                                              )
    res = data_manager.init_ingestion_src()
    ingestion_data['product_id'], ingestion_data['product_version'] = res['resource_name'].split('-')

    # define relative path to ingestion
    if config.SOURCE_DATA_PROVIDER_A.lower() == "pv" or config.SOURCE_DATA_PROVIDER_A.lower() == "pvc":
        discrete_raw_root_dir = config.DISCRETE_RAW_ROOT_DIR_CORE_A
    elif config.SOURCE_DATA_PROVIDER_A.lower() == "nfs" or config.SOURCE_DATA_PROVIDER_A.lower() == "fs":
        discrete_raw_root_dir = config.NFS_RAW_ROOT_DIR_CORE_A
    ingestion_dir = res['ingestion_dir'].split(discrete_raw_root_dir)[1]

    ValueStorage.discrete_list.append(ingestion_data)  # store running data for future cleanup
    _log.info(f'\nFinish prepare of ingestion data:\n'
              f'Source on dir: {res["ingestion_dir"]}\n'
              f'SourceId: {res["resource_name"]}\n'
              + stringy.pad_with_minus('End of ingestion data preparation') + '\n')

    # ============================================== Run ingestion =====================================================
    _log.info(
        '\n' + stringy.pad_with_stars('Start Discrete ingestion'))
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
        f'\n' + stringy.pad_with_minus('Discrete ingestion complete') + '\n')
    cleanup_data = {
        'product_id': ingestion_data['product_id'],
        'product_version': ingestion_data.get('product_version'),
        # "mapproxy_last_id": mapproxy_last_id,
        # "mapproxy_length": mapproxy_length,
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


def get_mapproxy_configuration(pg_params):
    """
    This method will return mapproxy configuration current details:
    :param pg_params: [dict] include db params -> {"entrypoint_url":[str] endpoint of pg server,
                                                 "port": [str] port number for server endpoint,
                                                 "pg_user": [str] credential for pg user name,
                                                 "pg_pass": [str] credential password for pg,
                                                 "pg_job_task_db": [str] table name for job tasks,
                                                 "pg_pycsw_db": [str] table name for pycsw records,
                                                 "pg_mapproxy_db": [str] table name for mapproxy configs,
                                                 "pg_agent_db": [str] table name for agent db
                                                }
    :return: [dict]
            'last id' -> the last id index of configs on mapproxy db
            'length' ->  the number of configs rows on mapproxy db
    """
    pg_credential = config.PGProvider(entrypoint_url=pg_params['entrypoint_url'],
                                      port=pg_params['port'],
                                      pg_user=pg_params['pg_user'],
                                      pg_pass=pg_params['pg_pass'],
                                      pg_job_task_db=pg_params['pg_job_task_db'],
                                      pg_pycsw_record_db=pg_params['pg_pycsw_db'],
                                      pg_mapproxy_db=pg_params['pg_mapproxy_db'],
                                      pg_agent_db=pg_params['pg_agent_db'])

    pg_handler = postgres_adapter.PostgresHandler(pg_credential)
    initial_mapproxy_configs = pg_handler.get_mapproxy_configs()
    mapproxy_last_id = initial_mapproxy_configs[0]['id']
    mapproxy_length = len(initial_mapproxy_configs)
    return {'last_id': mapproxy_last_id, 'length': mapproxy_length}


def count_tiles_amount(product_id, product_version, core):
    """
    This method counting actual amount transferred and exists on storage
    :param product_id: str -> resource id of the layer to sync
    :param product_version: version of discrete
    :param core: "A" [send] | "B" [received]
    :return: int -> total amount of tiles
    """
    _log.info(
        '\n\n' + stringy.pad_with_stars('Start tiles count on storage'))
    if core.lower() == "b":
        if config.TILES_PROVIDER_B.lower() == 's3':
            s3_credential = structs.S3Provider(entrypoint_url=config.S3_ENDPOINT_URL_CORE_B,
                                               access_key=config.S3_ACCESS_KEY_CORE_B,
                                               secret_key=config.S3_SECRET_KEY_CORE_B,
                                               bucket_name=config.S3_BUCKET_NAME_CORE_B)
            fs_provider = structs.FSProvider(is_fs=False)
        else:
            s3_credential = None
            fs_provider = structs.FSProvider(is_fs=True,
                                             tiles_dir=config.NFS_TILES_DIR_B)

        storage_provider = structs.StorageProvider(tiles_provider=config.TILES_PROVIDER_B,
                                                   s3_credential=s3_credential,
                                                   pvc_handler_url=config.PVC_HANDLER_URL_CORE_B,
                                                   fs_provider=fs_provider)
    elif core.lower() == "a":
        if config.TILES_PROVIDER_A.lower() == 's3':
            s3_credential = structs.S3Provider(entrypoint_url=config.S3_ENDPOINT_URL_CORE_A,
                                               access_key=config.S3_ACCESS_KEY_CORE_A,
                                               secret_key=config.S3_SECRET_KEY_CORE_A,
                                               bucket_name=config.S3_BUCKET_NAME_CORE_A)
            fs_provider = structs.FSProvider(is_fs=False)

        else:
            s3_credential = None
            fs_provider = structs.FSProvider(is_fs=True,
                                             tiles_dir=config.NFS_TILES_DIR_A)

        storage_provider = structs.StorageProvider(tiles_provider=config.TILES_PROVIDER_A,
                                                   s3_credential=s3_credential,
                                                   pvc_handler_url=config.PVC_HANDLER_URL_CORE_A,
                                                   fs_provider=fs_provider
                                                   )

    data_manager = data_executors.DataManager(watch=False, storage_provider=storage_provider)
    _log.info(f'Will execute tile search and count on:\n'
              f'Core: {core}\n'
              f'Tile storage provider: {storage_provider.get_tiles_provider()}')
    res = data_manager.count_tiles_on_storage(product_id, product_version)

    _log.info(
        f'\n' + stringy.pad_with_minus('End tiles count on storage') + '\n')
    return res


def trigger_orthphoto_history_sync(product_id, product_version):
    """
    This method will trigger new sync job for OrthophotoHistory type
    :param product_id: str -> resource id of the layer to sync
    :param product_version: version of discrete
    :return:
    """
    _log.info(
        '\n' + stringy.pad_with_stars('Start Triggering Sync for ingestion'))

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
        f'\n' + stringy.pad_with_minus('Finish Triggering Sync'))
    return {"state": state, "msg": msg}


# ============================================ job manager controllers ============-====================================
def creation_job_loop_follower(criteria):
    """
    This method will execute "validate_sync_job_creation" method based on loop + timeout excepting sync receiving job
    :param criteria: dict -> running criteria -> example:
    {
        product_id: str,
        product_id: product_version,
        job_type: str -> [SYNC_TRIGGER],
        job_manager_url: str -> url of job manager
    }
    :return: dict -> {state: bool, message: str, records: list[dict]}
    """
    end_process_string = '\n\n' + stringy.pad_with_minus('End Sync receiver loop')
    _log.info(
        f'\n' + stringy.pad_with_stars('Start Sync receiver loop'))

    timeout = criteria['timeout']
    product_id = criteria['product_id']
    product_version = criteria['product_version']
    job_type = criteria['job_type']
    job_manager_url = criteria['job_manager_url']

    retry_count = 1
    t_end = time.time() + timeout
    running = True
    while running:
        _log.info(f'Running search sync receive:\n'
                  f'{product_id}-{product_version}\n'
                  f'Job Type: {job_type}\n'
                  f'Search try iteration No. {retry_count}')

        res = validate_sync_job_creation(product_id=product_id,
                                         product_version=product_version,
                                         job_type=job_type,
                                         job_manager_url=job_manager_url)

        current_time = time.time()

        if res['state']:
            _log.info(f'{end_process_string}')
            return res

        elif t_end < current_time:
            _log.warning(f'Failed search receive job because of timeout')
            _log.info(f'{end_process_string}')
            res['message'] = 'Failed search receive job because of timeout'
            return res

        else:
            _log.info(f'Failed search receive job will try next iteration')
            retry_count += 1

        time.sleep(20)


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


def follow_sync_job(product_id, product_version, product_type, job_manager_url, running_timeout=300,
                    internal_timeout=80):
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
        '\n\n' + stringy.pad_with_stars('Start Follow Sync job'))
    _log.debug(f'Parameters for follow sync job:\n'
               f'Product ID: {product_id}\n'
               f'Product version: {product_id}\n'
               f'Product Type: {product_type}\n'
               f'Follow timeout bounds: {running_timeout} sec\n'
               f'Internal system delay: {internal_timeout} sec')

    job_manager_client = job_manager_api.JobsTasksManager(job_manager_url)
    res = job_manager_client.follow_running_job_manager(product_id=product_id,
                                                        product_version=product_version,
                                                        product_type=product_type,
                                                        timeout=running_timeout,
                                                        internal_timeout=internal_timeout)
    res["status"] = True if res["status"] == config.JobStatus.Completed.value else False
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
        f'\n' + stringy.pad_with_minus('Finish Follow Sync'))
    return res


# ============================================= core A validation ======================================================


def get_layer_spec_tile_count(layer_id, target, layer_spec_url):
    """
    This method query layer spec api and receive tile count as written after core A sync process and compare with
    expected value that provided.
    :param layer_id: "product_id-product_version"
    :param target: destination target
    :param layer_spec_url:url for layer spec route
    :return: dict -> {state:bool, msg:str}
    """
    _log.info(
        '\n\n' + stringy.pad_with_stars('Get actual tile count on layer spec'))
    try:
        layer_spec = layer_spec_api.LayerSpec(layer_spec_url)
        status_code, res = layer_spec.get_tiles_count(layer_id=layer_id,
                                                      target=target)

        _log.info(f'Request tile count on layer spec\n'
                  f'Status code: {status_code}\n'
                  f'Tiles count: {res}')
        if status_code != config.ResponseCode.Ok.value:
            res = {'state': False, 'message': f'Failed with error code: {status_code} and error message: [{res}]'}

        else:
            result = res.get('tilesCount')
            if result > 0:
                res = {'state': True, 'message': f'Layer spec include tiles count value: {result}',
                       'tile_count': result}
            else:
                res = {'state': False, 'message': f'Layer spec [{result}] are not > 0', 'tile_count': result}

    except Exception as e:
        res = {'state': False, 'message': f'Failed layer spec validation with error: [{str(e)}]', 'tile_count': None}

    _log.info(
        f'\n' + stringy.pad_with_minus('Finish layer spec tile count'))

    return res


def validate_toc_task_creation(job_id, expected_tiles_count, toc_job_type=config.JobTaskTypes.TOC_SYNC.value,
                               job_manager_endpoint_url=config.JOB_MANAGER_ROUTE_CORE_A):
    """
    The method validate core A toc task creation and validate num of tiles
    :param job_manager_endpoint_url: route url
    :param job_id: id of related job for toc task
    :param expected_tiles_count: original tile amount that was created on ingestion
    :param toc_job_type: The type as represented for toc type task
    :return: dict -> {'state': bool, 'reason': 'str, 'toc': dict}
    """
    param = {
        "jobId": job_id,
        "type": toc_job_type
    }
    _log.info(
        f'\n\n' + stringy.pad_with_stars('Start toc Sync validation'))
    _log.info(f'\nPrepare validation of tile count on toc for:\n'
              f'{param}\n'
              f'Expected tiles: {expected_tiles_count}')
    result = {}
    try:
        job_manager_client = job_manager_api.JobsTasksManager(job_manager_endpoint_url)
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

    _log.info(
        f'\n\n' + stringy.pad_with_minus('Finish toc Sync validation'))
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


def validate_metadata_pycsw(metadata, layer_id, layer_version, pycsw_url, query_params, sync_flag=True):
    """
    The method validate computability between metadata written to toc against the actual data on csw's records
    :param metadata: source metadata provided as dict -> {metadata: {...}}
    :param layer_id: id represent the layer [str]
    :param sync_flag: For sync pycsw validation -> based on toc metadata [bool]
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
    _log.info(
        f'\n\n' + stringy.pad_with_stars('Will run validation of toc metadata vs. pycsw record'))
    _log.info(f'Will execute catalog validation (pycsw) with original metadata (toc) with current details:\n'
              f'Catalog url: [{pycsw_url}]\n'
              f'Query param to catalog: [{json.dumps(query_params, indent=4)}]\n'
              f'Metadata: will be presented only for log "debug" level')

    _log.debug(f'Metadata from toc: {json.dumps(metadata, indent=4, ensure_ascii=False)}')
    try:
        pycsw_conn = pycsw_validator.PycswHandler(pycsw_url, query_params)
        toc_json = {'metadata': ShapeToJSON().create_metadata_from_toc(metadata['metadata'])}
        results = pycsw_conn.validate_pycsw(toc_json, layer_id, layer_version, sync_flag=sync_flag)
        res_dict = results['results']
        pycsw_records = results['pycsw_record']
        links = results['links']


    except Exception as e:
        _log.error(f'Failed validation of pycsw with error: [{str(e)}]')
        res_dict = {'validation': False, 'reason': str(e)}
        pycsw_records = {}
        links = {}

    _log.info(
        f'\n' + stringy.pad_with_minus('Finish validation of toc metadata vs. pycsw record'))

    return res_dict, pycsw_records, links


# ============================================= mapproxy controllers ===================================================


def validate_mapproxy_layer(pycsw_record, product_id, product_version, params=None):
    """
    This method will ensure the url's provided on mapproxy from pycsw
    :return: result dict -> {'validation': bool, 'reason':{}}, links -> dict
    """
    _log.info(
        f'\n\n' + stringy.pad_with_stars('Will run validation of layer mapproxy vs. pycsw record'))

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
    _log.info(
        f'\n' + stringy.pad_with_minus('Finish validation of layer mapproxy vs. pycsw record'))
    return res


def create_new_receive_source(source, destination=None):
    """
    This method generate new duplication of source folder with unique product ID
    :param source: source directory of layer's
    :param destination: if exists -> will copy to specified, else will duplicate to same root
    :return:
    """
    _log.info(stringy.pad_with_stars('Start generate tiles raw data for sync'))
    if not os.path.exists(source):
        raise FileNotFoundError(f'Source directory not exists: {source}')
    _log.info(f'Will duplicate source data tiles to new directory and generate new product ID')
    if destination:
        dst_directory = destination

    else:
        new_layer_name = "_".join(["test_receive", common.generate_datatime_zulu().replace('-', '_').replace(':', '_')])
        dst_directory = os.path.join(os.path.dirname(source), new_layer_name)

    try:
        _log.info(f'Start copy data for sync on receive core from [{source}] to [{dst_directory}]')
        command = f'cp -r {source}/. {dst_directory}'
        os.system(command)
        if os.path.exists(dst_directory):
            _log.info(f'Success copy and creation of test data on: {dst_directory}')
        else:
            raise IOError('Failed on creating sync receive upload data directory')

    except Exception as e2:
        _log.error(f'Failed copy files from {source} into {dst_directory} with error: [{str(e2)}]')
        raise e2

    toc_file = [f for f in glob.glob(f'{dst_directory}/**/*.json', recursive=True)][0]

    with open(toc_file, "r+") as fp:
        data = json.load(fp)
        data['metadata']['productId'] = new_layer_name
        fp.seek(0)  # <--- should reset file position to the beginning.
        json.dump(data, fp, indent=4, ensure_ascii=False)
        fp.truncate()

    return {"layer_name": new_layer_name, "layer_destination": dst_directory}


def create_receive_sync(tiles_root_dir, product_id, image_format='png', file_receive_api=config.FILE_RECEIVER_API_B):
    """
    This method receive root directory to tiles to be sync + toc json file
    :param tiles_root_dir: root dir should be full path till the tiles directory ->
        <full/path/to/tiles>/product_id/product_version/product_type/<z/x/y.png>
    :param product_id: name of layer to be synced
    :param image_format: tiles format on storage - default 'png'
    :param file_receive_api: GW file receiver url
    :return: dict -> results states
    """
    fr = FileReceiver(file_receive_api)
    layer_path = os.path.join(tiles_root_dir, product_id)
    if not os.path.exists(layer_path):
        raise FileNotFoundError(f'Missing source directory: {layer_path}')
    if not len(os.listdir(layer_path)):
        raise Exception(f'Test dir is empty: {layer_path}')

    tiles_path_list = [f for f in glob.glob(f'{layer_path}/**/*.{image_format}', recursive=True)]
    toc_file = [f for f in glob.glob(f'{layer_path}/**/*.json', recursive=True)][0]

    # upload tiles:
    for tile in tiles_path_list:
        tile_name = tile.split(tiles_root_dir+'/')[1]
        resp = fr.send_to_file_receiver(tile_name, open(tile, "rb").read())

    # upload toc:
    toc = json.load(open(toc_file))
    toc_name = toc_file.split(tiles_root_dir + '/')[1]
    resp = fr.send_to_file_receiver(toc_name, open(toc_file, 'rb').read())

    # todo -> add logs traffic process



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
        print('\nwill do cleanup for azure environment')
        return {'status': 'Need to be updated'}

    elif config.ENV_NAME == config.EnvironmentTypes.PROD.name:
        print('\nwill do cleanup for production environment')
        return {'status': 'Need to be updated'}

    else:
        raise ValueError(f'Illegal environment value type: {config.ENV_NAME}')
