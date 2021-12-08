import enum
import json

from mc_automation_tools import common


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

# ================================================= api's routes =======================================================
endpoints_routes = conf.get('api_routes')
TRIGGER_NIFI_ROUTE = endpoints_routes.get('trigger_nifi', 'https://')
SYNC_TRIGGER_API = endpoints_routes.get('sync_trigger_api', 'https://')
JOB_MANAGER_ROUTE = endpoints_routes.get('job_manager', 'https://')
LAYER_SPEC_ROUTE = endpoints_routes.get('layer_spec', 'https://')




# todo
"""
layer relative path
if orthophoto :
    productid/product version
else: productid/productversion/producttype
"""