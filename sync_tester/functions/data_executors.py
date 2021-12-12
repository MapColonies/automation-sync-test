"""
This module provide multiple execution function that related to discrete data -> copy, value change and etc.
"""
import json
import os
import logging
from sync_tester.configuration import config
from mc_automation_tools.ingestion_api import azure_pvc_api
_log = logging.getLogger('sync_tester.functions.data_executors')

class DataManager:
    """
    This class interface and provide functionality
    """
    def __init__(self, env, watch):
        self.__env = env
        self.__watch = watch
        self.__pvc_handler = azure_pvc_api.PVCHandler(config.PVC_HANDLER_URL, self.__watch)

    def init_ingestion_src(self):
        """
        This module will init new ingestion source folder.
        The prerequisites must have source folder with suitable data and destination folder for new data
        The method will duplicate and rename metadata shape file to unique running name
        :return:dict with ingestion_dir and resource_name
        """
        if self.__env == config.EnvironmentTypes.QA.name or self.__env == config.EnvironmentTypes.DEV.name:
            res = self.init_ingestion_src_pvc()
            return res
        # elif self.__env == config.EnvironmentTypes.PROD.name:
        #     src = os.path.join(config.NFS_ROOT_DIR, config.NFS_SOURCE_DIR)
        #     dst = os.path.join(config.NFS_ROOT_DIR_DEST, config.NFS_DEST_DIR)
        #     try:
        #         res = init_ingestion_src_fs(src, dst)
        #         return res
        #     except FileNotFoundError as e:
        #         raise e
        #     except Exception as e1:
        #         raise Exception(f'Failed generating testing directory with error: {str(e1)}')
        #
        # else:
        #     raise ValueError(f'Illegal environment value type: {env}')

    def init_ingestion_src_pvc(self):
        """
        This module will init new ingestion source folder inside pvc - only on azure.
        The prerequisites must have source folder with suitable data and destination folder for new data
        The method will duplicate and rename metadata shape file to unique running name
        :return:dict with ingestion_dir and resource_name
        """

        _log.info(f'Send request to pvc_handler for data copy')
        msg = self._copy_data()
        _log.info(f'Success of copy files and data: {msg["source"]} -> {msg["newDesination"]}')
        _log.info(f'Send request of change sourceId to unique')
        resource_id = self._change_unique_sourceId()
        _log.info(f'Success change sourceId -> current new sourceId: [{resource_id}]')
        if config.PVC_UPDATE_ZOOM:
            _log.info(f'Send request of changing max zoom level of discrete ingestion to -> max_zoom {config.MAX_ZOOM_LEVEL}')
            res = self._change_max_zoom_level()
            _log.info(f'Finish update zoom level: {res}')
        else:
            _log.info('No max zoom update required')

        return {'ingestion_dir': msg["newDesination"], 'resource_name': resource_id}

    def _copy_data(self):
        """
        This method will copy source data to destination data by pvc handler configuration
        :return: response body msg -> msg["source"] ,msg["newDesination"]
        """
        try:
            resp = self.__pvc_handler.create_new_ingestion_dir()
            if not resp.status_code == config.ResponseCode.ChangeOk.value:
                raise Exception(
                    f'Failed access pvc on source data cloning with error: [{resp.text}] and status: [{resp.status_code}]')
            msg = json.loads(resp.text)
            source_dir = msg["source"]
            new_dir = msg['newDesination']
            _log.info(
                f'[{resp.status_code}]: New test running directory was created from source data: {source_dir} into {new_dir}')
        except Exception as e:
            raise Exception(f'Failed access pvc on source data cloning with error: [{str(e)}]')
        return msg

    def _change_unique_sourceId(self):
        """
        This method will update and generate unique based time str -> sourceId (productId)
        :return: str -> new resourceId (productId)
        """
        try:
            resp = self.__pvc_handler.make_unique_shapedata()
            if not resp.status_code == config.ResponseCode.ChangeOk.value:
                raise Exception(
                    f'Failed access pvc on source data updating metadata.shp with error: [{resp.text}] and status: [{resp.status_code}]')
            resource_name = json.loads(resp.text)['source']
            _log.info(
                f'[{resp.status_code}]: metadata.shp was changed resource name: {resource_name}')

        except Exception as e:
            raise Exception(f'Failed access pvc on changing shape metadata: [{str(e)}]')
        return resource_name

    def _change_max_zoom_level(self):
        """
        This will change max zoom level of ingestion -> by zoom level integer value in config
        :return: dict with results
        """
        try:
            resp = self.__pvc_handler.change_max_zoom_tfw(config.MAX_ZOOM_LEVEL)
            if resp.status_code == config.ResponseCode.Ok.value:
                msg = json.loads(resp.text)["json_data"][0]["reason"]
                _log.info(
                    f'Max resolution changed successfully: [{msg}]')
            else:
                raise Exception(
                    f'Failed on updating zoom level with error: [{json.loads(resp.text)["message"]} | {json.loads(resp.text)["json_data"]}]')
        except Exception as e:
            raise IOError(f'Failed updating zoom max level with error: [{str(e)}]')
        return msg

    def init_ingestion_src_fs(src, dst, watch=False):
        """
            This module will init new ingestion source folder - for file system / NFS deployment.
            The prerequisites must have source folder with suitable data and destination folder for new data
            The method will duplicate and rename metadata shape file to unique running name
            :return:dict with ingestion_dir and resource_name
        """
        if watch:
            deletion_watch_dir = os.path.dirname(dst)
            if os.path.exists(deletion_watch_dir):
                command = f'rm -rf {deletion_watch_dir}/*'
                os.system(command)
        else:
            deletion_watch_dir = dst
            if os.path.exists(dst):
                command = f'rm -rf {deletion_watch_dir}'
                os.system(command)

        if not os.path.exists(src):
            raise FileNotFoundError(f'[{src}] directory not found')

        try:
            command = f'cp -r {src}/. {dst}'
            os.system(command)
            if os.path.exists(dst):
                _log.info(f'Success copy and creation of test data on: {dst}')
            else:
                raise IOError('Failed on creating ingestion directory')

        except Exception as e2:
            _log.error(f'Failed copy files from {src} into {dst} with error: [{str(e2)}]')
            raise e2

        try:
            # file = os.path.join(dst, config.SHAPES_PATH, config.SHAPE_METADATA_FILE)
            file = os.path.join(discrete_directory_loader.get_folder_path_by_name(dst, config.SHAPES_PATH),
                                config.SHAPE_METADATA_FILE)
            if config.FAILURE_FLAG:
                source_name = update_shape_fs_to_failure(file)
            else:
                source_name = update_shape_fs(file)
            _log.info(
                f'[{file}]:was changed resource name: {source_name}')
        except Exception as e:
            _log.error(f'Failed on updating shape file: {file} with error: {str(e)}')
            raise e

        if config.PVC_UPDATE_ZOOM:
            res = metadata_convertor.replace_discrete_resolution(dst,
                                                                 str(config.zoom_level_dict[config.MAX_ZOOM_TO_CHANGE]),
                                                                 'tfw')

        return {'ingestion_dir': dst, 'resource_name': source_name, 'max_resolution': res}

