import enum
import json

from mc_automation_tools import common
from mc_automation_tools.models import structs


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


# class JobTaskTypes(enum.Enum):
#     """
#     Job manager types of job
#     """
#     DISCRETE_TILING = 'Discrete-Tiling'
#     SYNC_TRIGGER = 'SYNC_TRIGGER'
#     TOC_SYNC = 'tocSync'
#     SYNC_TARGET = 'syncTarget'
#     TILE_LOADING = 'tileLoading'


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
DB_ACCESS = environment.get('access_db', False)
CORE_TARGET = environment.get('core_target', "target2")
ENV_NAME = environment.get('name', 'QA')
DEBUG = environment.get('debug', True)
DEBUG_LOG = environment.get('debug_log', True)
LOG_TO_FILE = environment.get('log_to_file', True)
LOG_OUTPUT_PATH = environment.get('log_output_path', '/tmp/auto-logs')

# =============================================== outer configurations =================================================
job_manager_enum = conf.get('job_manger_enum')


class JobTaskTypes(enum.Enum):
    """
    Job manager types of job
    """
    DISCRETE_TILING = job_manager_enum.get("discrete_tiling", "Discrete-Tiling")
    SYNC_TRIGGER = job_manager_enum.get("sync_trigger", "SYNC_TRIGGER")
    TOC_SYNC = job_manager_enum.get("toc_sync", "tocSync")
    SYNC_TARGET = job_manager_enum.get("sync_target", "syncTarget")
    TILE_LOADING = job_manager_enum.get("tile_loading", "tileLoading")


"""
# coreA [send core]
"""
# ==================================================== general =========================================================
conf_send_core = conf.get('send_core')
SOURCE_DATA_PROVIDER_A = conf_send_core.get('source_data_provider', "NFS")
TILES_PROVIDER_A = conf_send_core.get('tiles_provider', "S3")
SYNC_FROM_A_MANUAL = conf_send_core.get('manuel_sync', False)
SYNC_TIMEOUT_A = conf_send_core.get('sync_timeout', 300)
# ================================================== nifi routes =======================================================
nifi_a = conf_send_core.get('nifi_credentials_a')
TRIGGER_NIFI_ROUTE_CORE_A = nifi_a.get('trigger_nifi', 'https://')
NIFI_SYNC_TRIGGER_API_CORE_A = nifi_a.get('nifi_sync_trigger_api', '/synchronize/trigger')
NIFI_SYNC_STATUS_API_CORE_A = nifi_a.get('nifi_sync_status_api', '/synchronize/status')
NIFI_SYNC_FILE_RECEIVED_API_CORE_A = nifi_a.get('nifi_sync_file_recived_api', '/synchronize/fileRecived')
# ================================================= api's routes =======================================================
endpoints_routes_a = conf_send_core.get('api_routes_a')
PYCSW_URL_A = endpoints_routes_a.get('pycsw_url', "UNKNOWN")
PYCSW_GET_RASTER_RECORD_PARAMS_A = endpoints_routes_a.get('pycsw_get_raster_record_params', {})
JOB_MANAGER_ROUTE_CORE_A = endpoints_routes_a.get('job_manager', 'https://')
LAYER_SPEC_ROUTE_CORE_A = endpoints_routes_a.get('layer_spec', 'https://')

# ============================================= discrete ingestion =====================================================
_endpoints_discrete_ingestion_a = conf_send_core.get('discrete_ingestion_credential')
NFS_TILES_DIR_A = _endpoints_discrete_ingestion_a.get('nfs_tiles_dir', '/tmp')
DISCRETE_AGENT_CORE_A = _endpoints_discrete_ingestion_a.get('agent_url', 'https://')
PVC_HANDLER_URL_CORE_A = _endpoints_discrete_ingestion_a.get('pvc_handler_url', 'https://')
DISCRETE_RAW_ROOT_DIR_CORE_A = _endpoints_discrete_ingestion_a.get('discrete_raw_root_dir', '/tmp')
DISCRETE_RAW_SRC_DIR_CORE_A = _endpoints_discrete_ingestion_a.get('discrete_raw_src_dir', 'ingestion/1')
DISCRETE_RAW_DST_DIR_CORE_A = _endpoints_discrete_ingestion_a.get('discrete_raw_dst_dir', 'ingestion/2')
NFS_RAW_ROOT_DIR_CORE_A = _endpoints_discrete_ingestion_a.get('nfs_raw_root_dir', '/tmp')
NFS_RAW_SRC_DIR_CORE_A = _endpoints_discrete_ingestion_a.get('nfs_raw_src_dir', 'ingestion/1')
NFS_RAW_DST_DIR_CORE_A = _endpoints_discrete_ingestion_a.get('nfs_raw_dst_dir', 'ingestion/2')
PVC_UPDATE_ZOOM_CORE_A = _endpoints_discrete_ingestion_a.get('change_max_zoom_level', True)
UPDATE_ZOOM_CORE_A = _endpoints_discrete_ingestion_a.get('change_max_zoom_level', True)
MAX_ZOOM_LEVEL_CORE_A = _endpoints_discrete_ingestion_a.get('max_zoom_level', 4)
FAILURE_FLAG_CORE_A = _endpoints_discrete_ingestion_a.get('failure_tag', False)
INGESTION_TIMEOUT_CORE_A = _endpoints_discrete_ingestion_a.get('ingestion_timeout', 300)
BUFFER_TIMEOUT_CORE_A = _endpoints_discrete_ingestion_a.get('buffer_timeout', 70)

# ============================================== PG Credential =========================================================
_pg_credentials_a = conf_send_core.get('pg_credential')
PG_ENDPOINT_URL_CORE_A = _pg_credentials_a.get('pg_endpoint_url', 'https://')
PG_PORT_A = _pg_credentials_a.get('pg_port', 5432)
PG_USER_CORE_A = _pg_credentials_a.get('pg_user', None)
PG_PASS_CORE_A = _pg_credentials_a.get('pg_pass', None)
PG_JOB_TASK_DB_CORE_A = _pg_credentials_a.get('pg_job_task_db', 'ingestion/1')
PG_PYCSW_RECORD_DB_CORE_A = _pg_credentials_a.get('pg_pycsw_record_db', 'ingestion/2')
PG_MAPPROXY_DB_CORE_A = _pg_credentials_a.get('pg_mapproxy_db', '/tmp')
PG_AGENT_DB_CORE_A = _pg_credentials_a.get('pg_agent_db', 'ingestion/1')

# ============================================== S3 Credential =========================================================
_s3_credentials_a = conf_send_core.get('s3_credential')
S3_ENDPOINT_URL_CORE_A = _s3_credentials_a.get('s3_endpoint_url', 'https://')
S3_ACCESS_KEY_CORE_A = _s3_credentials_a.get('s3_access_key', None)
S3_SECRET_KEY_CORE_A = _s3_credentials_a.get('s3_secret_key', None)
S3_BUCKET_NAME_CORE_A = _s3_credentials_a.get('s3_bucket_name', 'UNKNOWN')

"""
# coreB [receive core]
"""

conf_receive_core = conf.get('receive_core')
RECEIVE_JOB_FIND_TIMEOUT_B = conf_receive_core.get('except_sync_job_timeout', 300)
PRODUCT_ID_B = conf_receive_core.get('product_id')
PRODUCT_VERSION_B = conf_receive_core.get('product_version')
TILES_PROVIDER_B = conf_receive_core.get('tiles_provider')
SYNC_TIMEOUT_B = conf_receive_core.get('sync_timeout', 300)
SYNC_SOURCE_LAYER_PATH_B = conf_receive_core.get('sync_source_layer_path', "/tmp/sync_test_data")
SYNC_SOURCE_NAME_B = conf_receive_core.get('sync_source_name', "")
# ================================================= api's routes =======================================================
endpoints_routes_b = conf_receive_core.get('api_routes_b')
FILE_RECEIVER_API_B = endpoints_routes_b.get('file_receiver', 'http')
TRIGGER_NIFI_ROUTE_CORE_B = endpoints_routes_b.get('trigger_nifi', 'https://')
NIFI_SYNC_TRIGGER_API_CORE_B = endpoints_routes_b.get('nifi_sync_trigger_api', '/synchronize/trigger')
NIFI_SYNC_STATUS_API_CORE_B = endpoints_routes_b.get('nifi_sync_status_api', '/synchronize/status')
NIFI_SYNC_FILE_RECEIVED_API_CORE_B = endpoints_routes_b.get('nifi_sync_file_recived_api', '/synchronize/fileRecived')

JOB_MANAGER_ROUTE_CORE_B = endpoints_routes_b.get('job_manager', 'https://')
LAYER_SPEC_ROUTE_CORE_B = endpoints_routes_b.get('layer_spec', 'https://')
MAPPROXY_ROUTE_CORE_B = endpoints_routes_b.get('mapproxy_url', 'https://')

# ============================================= discrete ingestion =====================================================
_endpoints_discrete_ingestion_b = conf_receive_core.get('discrete_ingestion_credential_b')
PVC_HANDLER_URL_CORE_B = _endpoints_discrete_ingestion_b.get('pvc_handler_url', 'https://')
PYCSW_URL_B = _endpoints_discrete_ingestion_b.get('pycsw_url', "UNKNOWN")
PYCSW_GET_RASTER_RECORD_PARAMS_B = _endpoints_discrete_ingestion_b.get('pycsw_get_raster_record_params', {})
UPDATE_ZOOM_CORE_B = _endpoints_discrete_ingestion_b.get('change_max_zoom_level', True)
MAPPROXY_GRID_ORIGIN_B = _endpoints_discrete_ingestion_b.get('mapproxy_grid_origin', True)
NFS_TILES_DIR_B = _endpoints_discrete_ingestion_b.get('nfs_tiles_dir', '/tmp')
BUFFER_TIMEOUT_CORE_B = _endpoints_discrete_ingestion_b.get('buffer_timeout', 70)

# ============================================== PG Credential =========================================================
PG_CONFIG_CORE_B = conf_receive_core.get('pg_connection_b')
_pg_credentials_b = conf_receive_core.get('pg_credential_b')
PG_ENDPOINT_URL_CORE_B = _pg_credentials_b.get('pg_endpoint_url', 'https://')
PG_USER_CORE_B = _pg_credentials_b.get('pg_user', None)
PG_PASS_CORE_B = _pg_credentials_b.get('pg_pass', None)
PG_JOB_TASK_DB_CORE_B = _pg_credentials_b.get('pg_job_task_db', 'ingestion/1')
PG_PYCSW_RECORD_DB_CORE_B = _pg_credentials_b.get('pg_pycsw_record_db', 'ingestion/2')
PG_MAPPROXY_DB_CORE_B = _pg_credentials_b.get('pg_mapproxy_db', '/tmp')
PG_AGENT_DB_CORE_B = _pg_credentials_b.get('pg_agent_db', 'ingestion/1')

# ============================================== S3 Credential =========================================================
_s3_credentials_b = conf_receive_core.get('s3_credential_b')
S3_ENDPOINT_URL_CORE_B = _s3_credentials_b.get('s3_endpoint_url', 'https://')
S3_ACCESS_KEY_CORE_B = _s3_credentials_b.get('s3_access_key', None)
S3_SECRET_KEY_CORE_B = _s3_credentials_b.get('s3_secret_key', None)
S3_BUCKET_NAME_CORE_B = _s3_credentials_b.get('s3_bucket_name', 'UNKNOWN')

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

running_info = {
    "type": "sender",
    "date": "zulu time",
    "watch_state": False,
    "db": {
        "job_tasks": "job_task_db",
        "mapproxy_config": "mapproxy_db",
        "layer_spec": "layer_spec_db",
        "catalog": "catalog_db",
        "endpoint_url": "http",
        "user": "dsds",
        "password": "fdsfds"
    },
    "raw_data_source": {
        "provider": "FS",
        "full_path": "/tmp/ingestion"
    },
    "tiles_data": {
        "provider": "S3",
        "full_path": "/tiles",
        "s3_endpoint_url": None,
        "s3_user": None,
        "s3_pass": None,
        "s3_bucket": None
    },
    "layer": {
        "product_id": "fsdfsf",
        "product_version": "rewrewr"
    }
}

"""
layer relative path
if orthophoto :
    productid/product version
else: productid/productversion/producttype
"""


class PGProvider:
    """This class provide PG credential """

    def __init__(self, entrypoint_url, pg_user, pg_pass, pg_job_task_db, pg_pycsw_record_db, pg_mapproxy_db,
                 pg_agent_db, port=5432):
        self.pg_entrypoint_url = str(entrypoint_url)
        self.pg_port = port
        self.pg_user = str(pg_user)
        self.pg_pass = str(pg_pass)
        self.pg_job_task_db = str(pg_job_task_db)
        self.pg_pycsw_record_db = str(pg_pycsw_record_db)
        self.pg_mapproxy_db = str(pg_mapproxy_db)
        self.pg_agent_db = str(pg_agent_db)


class S3Provider:
    """This class provide s3 credential """

    def __init__(self, entrypoint_url, access_key, secret_key, bucket_name=None):
        self.s3_entrypoint_url = entrypoint_url
        self.s3_access_key = access_key
        self.s3_secret_key = secret_key
        self.s3_bucket_name = bucket_name

    def get_entrypoint_url(self):
        return self.s3_entrypoint_url

    def get_access_key(self):
        return self.s3_access_key

    def get_secret_key(self):
        return self.s3_secret_key

    def get_bucket_name(self):
        return self.s3_bucket_name


class StorageProvider:
    """This class provide gathered access to core's storage credential """

    def __init__(self,
                 source_data_provider=None,
                 tiles_provider=None,
                 s3_credential=None,
                 pvc_handler_url=None):
        self.__source_data_provider = source_data_provider
        self.__tiles_provider = tiles_provider
        self.s3_credential = s3_credential
        self.__pvc_handler_url = pvc_handler_url

    def get_source_data_provider(self):
        return self.__source_data_provider

    def get_tiles_provider(self):
        return self.__tiles_provider

    def get_s3_credential(self):
        return self.s3_credential

    def get_pvc_url(self):
        return self.__pvc_handler_url


class MapproxyProvider:
    """
    Include all param for mapproxy validator
    """

    def __init__(self, entrypoint, tile_storage_provide, grid_origin="ul", s3_credential=None, nfs_tiles_url=None):
        self.__entrypoint = entrypoint
        self.__tile_storage_provide = tile_storage_provide
        self.__grid_origin = grid_origin
        self.__s3_credential = s3_credential
        self.__nfs_tiles_url = nfs_tiles_url
