"""This module will provide main functionality to run discrete ingestion"""
import logging
import json
from sync_tester.configuration import config
from mc_automation_tools.ingestion_api import agent_api
_log = logging.getLogger('sync_tester.functions.discrete_ingestion_executors')
# agent_ins = agent_api.DiscreteAgentApi(config.DISCRETE_JOB_MANAGER_URL_CORE_A)

# =============================================== Agent API's functions ================================================


class DiscreteAgentAdapter:
    def __init__(self, entrypoint_url, source_data_provider="NFS", discrete_raw_root_dir=""):
        self.__entrypoint_url = entrypoint_url
        self.__source_data_provider = source_data_provider
        self.__discrete_raw_root_dir = discrete_raw_root_dir
        self.__conn = agent_api.DiscreteAgentApi(self.__entrypoint_url)

    def get_watch_status(self):
        """
        This method stop and validate stop / status api of agent for watching
        :return: result dict: {state: bool, reason: str}
        """
        try:
            resp = self.__conn.get_watching_statuses()
            status_code = resp.status_code
            if status_code != config.ResponseCode.Ok.value:
                return {'state': False,
                        'reason': f'Failed on getting watch status API after changing: [{status_code}]:[{resp.content}]'}

            return {'state': True, 'reason': f'{json.loads(resp.content)}'}
        except Exception as e:
            _log.error(f'Failed on get watching status with error: [{str(e)}]')
            raise Exception(f'Failed on get watching status with error: [{str(e)}]')

    def stop_agent_watch(self):
        """
        This method stop and validate stop / status api of agent for watching
        :return: result dict: {state: bool, reason: str}
        """
        try:
            resp = self.__conn.post_stop_watch()
            status_code = resp.status_code
            if status_code != config.ResponseCode.Ok.value:
                return {'state': False, 'reason': f'Failed on post stop watch on API: [{status_code}]:[{resp.content}]'}

            reason = json.loads(resp.content)
            if reason['isWatching']:
                return {'state': False, 'reason': f'Failed on stop watch via status API: [{json.loads(resp.content)}]'}

            return {'state': True, 'reason': 'isWatch=False - agent not watching'}
        except Exception as e:
            _log.error(f'Failed on stop watching process with error: [{str(e)}]')
            raise Exception(f'Failed on stop watching process with error: [{str(e)}]')

    def start_agent_watch(self):
        """
        This method stop and validate stop / status api of agent for watching
        :return: result dict: {state: bool, reason: str}
        """
        try:
            resp = self.__conn.post_start_watch()
            status_code = resp.status_code
            if status_code != config.ResponseCode.Ok.value:
                return {'state': False, 'reason': f'Failed on post start watch on API: [{status_code}]:[{resp.content}]'}

            reason = json.loads(resp.content)
            if not reason['isWatching']:
                return {'state': False, 'reason': f'Failed on start watch via status API: [{json.loads(resp.content)}]'}

            return {'state': True, 'reason': 'isWatch=True - agent is watching'}
        except Exception as e:
            _log.error(f'Failed on stop watching process with error: [{str(e)}]')
            raise Exception(f'Failed on stop watching process with error: [{str(e)}]')

    def send_agent_manual_ingest(self, source_dir):
        """
        This method will trigger manual ingestion by providing relative path for ingestion
        :param source_dir: relative path to discrete raw + meta data
        :return: uuid [jobId]
        """
        # if config.ENV_NAME == config.EnvironmentTypes.QA.name or config.ENV_NAME == config.EnvironmentTypes.DEV.name:
        if self.__source_data_provider.lower() == "pv" or self.__source_data_provider.lower() == "pvc":
            relative_path = source_dir.split(self.__discrete_raw_root_dir)[1]

        # elif config.ENV_NAME == config.EnvironmentTypes.PROD.name:
        elif self.__source_data_provider.lower() == "nfs" or self.__source_data_provider.lower() == "fs":
            relative_path = self.__discrete_raw_root_dir

        else:
            raise ValueError(f"Wrong source data provider was given: [{self.__source_data_provider}]")

        resp = self.__conn.post_manual_trigger(relative_path)
        status_code = resp.status_code
        content = resp.text
        return status_code, content



