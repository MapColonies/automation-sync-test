import enum
import json

from mc_automation_tools import common

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
    orthphoto = 'orthophoto'
    orthophoto_history = 'orthophotoHistory'


JOB_MANAGER_TYPE = {
    'discrete_tiling': 'Discrete-Tiling'
}


CONF_FILE = common.get_environment_variable('CONF_FILE', None)
if not CONF_FILE:
    raise EnvironmentError('Should provide path for CONF_FILE')

with open(CONF_FILE, 'r') as fp:
    conf = json.load(fp)


# ========================================== environments configurations ===============================================
environment = conf.get('environment')
ENV_NAME = environment.get('name', 'QA')
SYNC_TYPE = environment.get('sync_type', 'QA')
DEBUG = environment.get('debug', True)
DEBUG_LOG = environment.get('debug_log', True)
STORAGE = environment.get('storage', "S3")
STORAGE_ADAPTER = environment.get('storage_adapter', "PVC"),
STORAGE_TILES = environment.get('storage_tiles', "S3")
TILES_RELATIVE_PATH = environment.get('tiles_relative_path', "tiles")

# ================================================= api's routes =======================================================
endpoints_routes = conf.get('api_routes')
TRIGGER_NIFI_ROUTE = endpoints_routes.get('trigger_nifi', 'https://')
SYNC_TRIGGER_API = endpoints_routes.get('sync_trigger_api', 'https://')
JOB_MANAGER_ROUTE = endpoints_routes.get('job_manager', 'https://')
LAYER_SPEC_ROUTE = endpoints_routes.get('layer_spec', 'https://')

# ============================================= discrete ingestion =====================================================
_endpoints_discrete_ingestion = conf.get('discrete_ingestion_credential')
DISCRETE_JOB_MANAGER_URL = _endpoints_discrete_ingestion.get('agent_url', 'https://')
PVC_HANDLER_URL = _endpoints_discrete_ingestion.get('pvc_handler_url', 'https://')
DISCRETE_RAW_ROOT_DIR = _endpoints_discrete_ingestion.get('discrete_raw_root_dir', '/tmp')
DISCRETE_RAW_SRC_DIR = _endpoints_discrete_ingestion.get('discrete_raw_src_dir', 'ingestion/1')
DISCRETE_RAW_DST_DIR = _endpoints_discrete_ingestion.get('discrete_raw_dst_dir', 'ingestion/2')
NFS_RAW_ROOT_DIR = _endpoints_discrete_ingestion.get('nfs_raw_root_dir', '/tmp')
NFS_RAW_SRC_DIR = _endpoints_discrete_ingestion.get('nfs_raw_src_dir', 'ingestion/1')
NFS_RAW_DST_DIR = _endpoints_discrete_ingestion.get('nfs_raw_dst_dir', 'ingestion/2')
PVC_UPDATE_ZOOM = _endpoints_discrete_ingestion.get('change_max_zoom_level', True)
MAX_ZOOM_LEVEL = _endpoints_discrete_ingestion.get('max_zoom_level', 4)
FAILURE_FLAG = _endpoints_discrete_ingestion.get('failure_tag', False)
INGESTION_TIMEOUT = _endpoints_discrete_ingestion.get('ingestion_timeout', 300)
BUFFER_TIMEOUT = _endpoints_discrete_ingestion.get('buffer_timeout', 70)

# ============================================== PG Credential =========================================================
_pg_credentials = conf.get('pg_credential')
PG_ENDPOINT_URL = _pg_credentials.get('pg_endpoint_url', 'https://')
PG_USER = _pg_credentials.get('pg_user', None)
PG_PASS = _pg_credentials.get('pg_pass', None)
PG_JOB_TASK_TABLE = _pg_credentials.get('pg_job_task_table', 'ingestion/1')
PG_PYCSW_RECORD_TABLE = _pg_credentials.get('pg_pycsw_record_table', 'ingestion/2')
PG_MAPPROXY_TABLE = _pg_credentials.get('pg_mapproxy_table', '/tmp')
PG_AGENT_TABLE = _pg_credentials.get('pg_agent_table', 'ingestion/1')

# ============================================== S3 Credential =========================================================
_s3_credentials = conf.get('s3_credential')
S3_ENDPOINT_URL = _s3_credentials.get('s3_endpoint_url', 'https://')
S3_ACCESS_KEY = _s3_credentials.get('s3_access_key', None)
S3_SECRET_KEY = _s3_credentials.get('s3_secret_key', None)
S3_BUCKET_NAME = _s3_credentials.get('s3_bucket_name', 'UNKNOWN')


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

# todo
"""
layer relative path
if orthophoto :
    productid/product version
else: productid/productversion/producttype
"""