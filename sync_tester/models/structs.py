"""
This module provide common struct & class
"""
from mc_automation_tools.models import structs
import enum


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


class JobStatus(enum.Enum):
    """
    Types of job statuses
    """
    Completed = 'Completed'
    Failed = 'Failed'
    InProgress = 'In-Progress'
    Pending = 'Pending'


class MapProtocolType(enum.Enum):
    """
    Types of orthophoto access protocol statuses
    """
    WMS = 'WMS'
    WMTS = 'WMTS'
    WMTS_LAYER = 'WMTS_LAYER'

class tile_storage_provider(enum.Enum):
    S3 = "S3"
    FS = "FS"
    PV = "PV"
