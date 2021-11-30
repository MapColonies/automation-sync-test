"""
This module is the wrapper for job manager api according provided swagger
"""
import json
from sync_tester.configuration import config
from mc_automation_tools import base_requests, common


class JobsTasksManager:
    __jobs_api = 'jobs'
    __resettable = 'resettable'
    __reset = 'reset'
    __tasks = 'tasks'

    def __init__(self, end_point_url):
        self.__end_point_url = end_point_url

    @property
    def get_class_params(self):
        params = {
            'jobs_api': self.__jobs_api,
            'resettable': self.__resettable,
            'reset': self.__reset,
            'tasks': self.__tasks
        }
        return params

    def find_jobs_by_criteria(self, params):
        """
        This method will query and return information about jobs and tasks on job manager db by providing query params
        * user should provide dict to parameters according the api:
            {
                'resourceId': string,
                'version': string,
                'isCleaned': bool,
                'status': string -> [Pending, In-Progress, Completed, Failed],
                'type': string -> [The type of the job],
                'shouldReturnTasks': bool -> default is True

            }
        :param params: query params -> resourceId, version, isCleaned, status, type, shouldReturnTasks
        :return: full jobs status + tasks by params
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api)
        if isinstance(params, dict):
            params = json.dumps(params)
        elif not isinstance(params, str):
            raise ValueError(f'params is not on valid params -> json or dict')
        resp = base_requests.send_get_request2(url, params)
        if resp.status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[find_jobs_by_criteria]:failed retrieve jobs, return with error:[{resp.status_code}],error msg:[{str(resp.content)}]')

        return json.loads(resp.content)

    def create_new_job(self, body):
        """
        This method will write new job into DB of job manager -> jobs table, example:
        {
          "resourceId": "string",
          "version": "string",
          "description": "string",
          "parameters": {},
          "status": "Pending",
          "reason": "string",
          "type": "string",
          "percentage": 100,
          "priority": 0,
          "expirationDate": "2021-11-30T11:59:56.954Z",
          "tasks": [
            {
              "description": "string",
              "parameters": {},
              "reason": "string",
              "percentage": 100,
              "type": "string",
              "status": "Pending",
              "attempts": 0
            }
          ]
        }
        :param body: json body of requested params
        :return: json of job id and related task ids
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api)
        if isinstance(body, dict):
            body = json.dumps(body)
        elif not isinstance(body, str):
            raise ValueError(f'params is not on valid params -> json or dict')

        resp = base_requests.send_post_request(url, body)
        if resp.status_code != config.ResponseCode.ChangeOk.value:
            raise Exception(
                f'[create_new_job]:failed on creation new job, return with error:[{resp.status_code}],error msg:[{str(resp.content)}]')
        return json.loads(resp.content)

    def get_job_by_id(self, uuid, return_tasks=True):
        """
        This method will query specific job according its uuid
        :param uuid: str -> unique id of job provided on creation
        :param return_tasks: bool -> Flag if provide with response also the tasks related
        :return: json\ dict of the job
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api, uuid)
        params = json.dumps({'shouldReturnTasks': return_tasks})
        resp = base_requests.send_get_request2(url, params)
        if resp.status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[get_job_by_id]:failed retrieve job, return with error:[{resp.status_code}]:error msg:[{str(resp.content)}]')

        return json.loads(resp.content)

    def updates_job(self, uuid, body):
        """
        This method based on PUT rest request to update exists job
        example of body:
            {
              "parameters": {},
              "status": "Pending",
              "percentage": 100,
              "reason": "string",
              "isCleaned": true,
              "priority": 0,
              "expirationDate": "2021-11-30T12:27:46.951Z"
            }
        :param uuid: str -> unique id of job provided on creation
        :param body: dict -> entire parameters to change
        :return: success message
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api, uuid)
        if isinstance(body, dict):
            body = json.dumps(body)
        elif not isinstance(body, str):
            raise ValueError(f'params is not on valid params -> json or dict')
        resp = base_requests.send_put_request(url, body)
        if resp.status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[get_job_by_id]:failed update job, return with error:[{resp.status_code}]:error msg:[{str(resp.content)}]')
        return resp.text

    def delete_job(self, uuid, return_tasks=True):
        """
        This method will delete job from job manager db based on provided uuid
        :param uuid: str -> unique id of job provided on creation
        :param return_tasks: bool -> Flag if provide with response also the tasks related
        :return: response state
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api, uuid)
        params = json.dumps({'shouldReturnTasks': return_tasks})
        resp = base_requests.send_delete_request(url, params)
        if resp.status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[delete_job]:failed delete job, return with error:[{resp.status_code}]:error msg:[{str(resp.content)}]')

        return json.loads(resp.content)

    def resettable(self, uuid):
        """
        This method check if job is resettable
        :param uuid: str -> unique id of job provided on creation
        :return: dict -> {jobId:str, isResettable:bool} on success
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api, uuid, self.__resettable)
        resp = base_requests.send_post_request(url)
        if resp.status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[resettable]:failed get resettable state for job, return with error:[{resp.status_code}]:error msg:[{str(resp.content)}]')

        return json.loads(resp.content)

    def resettable(self, uuid):
        """
        This method reset a resettable job
        :param uuid: str -> unique id of job provided on creation
        :return: dict -> {jobId:str, isResettable:bool} on success
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api, uuid, self.__reset)
        resp = base_requests.send_post_request(url)
        if resp.status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[reset]:failed start reset on resettable job, return with error:[{resp.status_code}]:error msg:[{str(resp.content)}]')

        return str(resp.text)

    def tasks(self, uuid):
        """
        This method will get all tasks under provided id of job
        :param uuid: str -> unique id of job provided on creation
        :return: list[dict] -> list of dicts representing all tasks under provided job
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api, uuid, self.__tasks)
        resp = base_requests.send_get_request2(url)
        if resp.status_code != config.ResponseCode.Ok.value:
            raise Exception(
                f'[tasks]:failed get tasks of job, return with error:[{resp.status_code}]:error msg:[{str(resp.content)}]')

        return json.loads(resp.content)

    def create_task(self, uuid, body):
        """
        This method will get all tasks under provided id of job
        * example of body:
            {
              "description": "string",
              "parameters": {},
              "reason": "string",
              "percentage": 100,
              "type": "string",
              "status": "Pending",
              "attempts": 0
            }
        :param uuid: str -> unique id of job provided on creation
        :param body: dict -> body contain of the fields to insert on new task
        :return: dict -> id: <new id of the created task>
        """
        url = common.combine_url(self.__end_point_url, self.__jobs_api, uuid, self.__tasks)
        # if isinstance(body, dict):
        #     body = json.dumps(body)
        # elif not isinstance(body, str):
        #     raise ValueError(f'params is not on valid params -> json or dict')
        resp = base_requests.send_post_request(url, body)
        if resp.status_code != config.ResponseCode.ChangeOk.value:
            raise Exception(
                f'[create_task]:failed insert new task to job, return with error:[{resp.status_code}]:error msg:[{str(resp.content)}]')

        return json.loads(resp.content)