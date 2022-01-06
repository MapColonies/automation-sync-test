import enum
import json

from mc_automation_tools import common


class JobStatus(enum.Enum):
    """
    Types of job statuses
    """
    Completed = 'Completed'
    Failed = 'Failed'
    InProgress = 'In-Progress'
    Pending = 'Pending'


class EnvironmentTypes(enum.Enum):
    """
    Types of environment.
    """
    QA = 1
    DEV = 2
    PROD = 3


class ResponseCode(enum.Enum):
    """
    Types of server responses
    """
    Ok = 200  # server return ok status
    ChangeOk = 201  # server was return ok for changing
    NoJob = 204  # No job
    ValidationErrors = 400  # bad request
    StatusNotFound = 404  # status\es not found on db
    DuplicatedError = 409  # in case of requesting package with same name already exists
    GetwayTimeOut = 504  # some server didn't respond
    ServerError = 500  # problem with error


class JobTypes(enum.Enum):
    """
    Job manager types of job
    """
    DISCRETE_TILING = 'Discrete-Tiling'
    SYNC_TRIGGER = 'SYNC_TRIGGER'
    TOC_SYNC = 'tocSync'


class SyncOperationType(enum.Enum):
    """
    Types of sync operation
    """
    ADD = 'ADD'
    UPDATE = 'UPDATE'
    REMOVE = 'REMOVE'


class ProductType(enum.Enum):
    """
    Types of products
    """
    orthphoto = 'Orthophoto'
    orthophoto_history = 'OrthophotoHistory'


CONF_FILE = common.get_environment_variable('CONF_FILE', None)
if not CONF_FILE:
    raise EnvironmentError('Should provide path for CONF_FILE')

with open(CONF_FILE, 'r') as fp:
    conf = json.load(fp)


# ========================================== environments configurations ===============================================
environment = conf.get('environment')
SYNC_FROM_A_MANUAL = environment.get('manual_sync', True)
SYNC_TIMEOUT = environment.get('sync_timeout', 300)
CORE_TARGET = environment.get('core_target', "target2")
ENV_NAME = environment.get('name', 'QA')
SYNC_TYPE = environment.get('sync_type', 'QA')
DEBUG = environment.get('debug', True)
DEBUG_LOG = environment.get('debug_log', True)
LOG_TO_FILE = environment.get('log_to_file', True)
LOG_OUTPUT_PATH = environment.get('log_output_path', '/tmp/auto-logs')
STORAGE = environment.get('storage', "S3")
STORAGE_ADAPTER = environment.get('storage_adapter', "PVC"),
STORAGE_TILES = environment.get('storage_tiles', "S3")
TILES_RELATIVE_PATH = environment.get('tiles_relative_path', "tiles")


"""
# coreA [send core]
"""

conf_send_core = conf.get('send_core')
# ================================================= api's routes =======================================================
endpoints_routes = conf_send_core.get('api_routes')
TRIGGER_NIFI_ROUTE_CORE_A = endpoints_routes.get('trigger_nifi', 'https://')
NIFI_SYNC_TRIGGER_API_CORE_A = endpoints_routes.get('nifi_sync_trigger_api', '/synchronize/trigger')
NIFI_SYNC_STATUS_API_CORE_A = endpoints_routes.get('nifi_sync_status_api', '/synchronize/status')
NIFI_SYNC_FILE_RECEIVED_API_CORE_A = endpoints_routes.get('nifi_sync_file_recived_api', '/synchronize/fileRecived')

JOB_MANAGER_ROUTE_CORE_A = endpoints_routes.get('job_manager', 'https://')
LAYER_SPEC_ROUTE = endpoints_routes.get('layer_spec', 'https://')

# ============================================= discrete ingestion =====================================================
_endpoints_discrete_ingestion = conf_send_core.get('discrete_ingestion_credential')
DISCRETE_JOB_MANAGER_URL_CORE_A = _endpoints_discrete_ingestion.get('agent_url', 'https://')
PVC_HANDLER_URL_CORE_A = _endpoints_discrete_ingestion.get('pvc_handler_url', 'https://')
DISCRETE_RAW_ROOT_DIR_CORE_A = _endpoints_discrete_ingestion.get('discrete_raw_root_dir', '/tmp')
DISCRETE_RAW_SRC_DIR_CORE_A = _endpoints_discrete_ingestion.get('discrete_raw_src_dir', 'ingestion/1')
DISCRETE_RAW_DST_DIR_CORE_A = _endpoints_discrete_ingestion.get('discrete_raw_dst_dir', 'ingestion/2')
NFS_RAW_ROOT_DIR_CORE_A = _endpoints_discrete_ingestion.get('nfs_raw_root_dir', '/tmp')
NFS_RAW_SRC_DIR_CORE_A = _endpoints_discrete_ingestion.get('nfs_raw_src_dir', 'ingestion/1')
NFS_RAW_DST_DIR_CORE_A = _endpoints_discrete_ingestion.get('nfs_raw_dst_dir', 'ingestion/2')
PVC_UPDATE_ZOOM_CORE_A = _endpoints_discrete_ingestion.get('change_max_zoom_level', True)
MAX_ZOOM_LEVEL_CORE_A = _endpoints_discrete_ingestion.get('max_zoom_level', 4)
FAILURE_FLAG_CORE_A = _endpoints_discrete_ingestion.get('failure_tag', False)
INGESTION_TIMEOUT_CORE_A = _endpoints_discrete_ingestion.get('ingestion_timeout', 300)
BUFFER_TIMEOUT_CORE_A = _endpoints_discrete_ingestion.get('buffer_timeout', 70)

# ============================================== PG Credential =========================================================
_pg_credentials = conf_send_core.get('pg_credential')
PG_ENDPOINT_URL_CORE_A = _pg_credentials.get('pg_endpoint_url', 'https://')
PG_USER_CORE_A = _pg_credentials.get('pg_user', None)
PG_PASS_CORE_A = _pg_credentials.get('pg_pass', None)
PG_JOB_TASK_DB_CORE_A = _pg_credentials.get('pg_job_task_db', 'ingestion/1')
PG_PYCSW_RECORD_DB_CORE_A = _pg_credentials.get('pg_pycsw_record_db', 'ingestion/2')
PG_MAPPROXY_DB_CORE_A = _pg_credentials.get('pg_mapproxy_db', '/tmp')
PG_AGENT_DB_CORE_A = _pg_credentials.get('pg_agent_db', 'ingestion/1')

# ============================================== S3 Credential =========================================================
_s3_credentials = conf_send_core.get('s3_credential')
S3_ENDPOINT_URL_CORE_A = _s3_credentials.get('s3_endpoint_url', 'https://')
S3_ACCESS_KEY_CORE_A = _s3_credentials.get('s3_access_key', None)
S3_SECRET_KEY_CORE_A = _s3_credentials.get('s3_secret_key', None)
S3_BUCKET_NAME_CORE_A = _s3_credentials.get('s3_bucket_name', 'UNKNOWN')


# mapping of zoom level and related resolution values
zoom_level_dict = {
    0: 0.703125,
    1: 0.3515625,
    2: 0.17578125,
    3: 0.087890625,
    4: 0.0439453125,
    5: 0.02197265625,
    6: 0.010986328125,
    7: 0.0054931640625,
    8: 0.00274658203125,
    9: 0.001373291015625,
    10: 0.0006866455078125,
    11: 0.00034332275390625,
    12: 0.000171661376953125,
    13: 0.0000858306884765625,
    14: 0.0000429153442382812,
    15: 0.0000214576721191406,
    16: 0.0000107288360595703,
    17: 0.00000536441802978516,
    18: 0.00000268220901489258,
    19: 0.00000134110450744629,
    20: 0.000000670552253723145,
    21: 0.000000335276126861572,
    22: 0.000000167638063430786
}


cleanup_json = {
  "product_id": "2021_12_14T13_10_45Z_MAS_6_ORT_247557",
  "product_version": "4.0",
  "mapproxy_last_id": 1,
  "mapproxy_length": 1,
  "folder_to_delete": "/tmp/danny_delete/watch",
  "tiles_folder_to_delete": "adsa",
  "watch_state": "False",
  "volume_mode": "nfs"
}

"""
layer relative path
if orthophoto :
    productid/product version
else: productid/productversion/producttype
"""

