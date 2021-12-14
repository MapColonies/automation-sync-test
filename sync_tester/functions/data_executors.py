"""
This module provide multiple execution function that related to discrete data -> copy, value change and etc.
"""
import glob
import json
import os
import logging
from sync_tester.configuration import config
from mc_automation_tools import shape_convertor, common, s3storage
from mc_automation_tools.ingestion_api import azure_pvc_api
from discrete_kit.functions import metadata_convertor, shape_functions
from discrete_kit.configuration import config as cfg
_log = logging.getLogger('sync_tester.functions.data_executors')


class DataManager:
    """
    This class interface and provide functionality
    """
    __shape = 'Shape'
    __shape_metadata_file = 'ShapeMetadata.shp'
    __tif = 'tiff'

    def __init__(self, env, watch):
        self.__env = env
        self.__watch = watch
        self.__pvc_handler = azure_pvc_api.PVCHandler(config.PVC_HANDLER_URL, self.__watch)
        self._dst_dir = None
        self._src_dir = None

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
        elif self.__env == config.EnvironmentTypes.PROD.name:
            src = os.path.join(config.NFS_RAW_ROOT_DIR, config.NFS_RAW_SRC_DIR)
            dst = os.path.join(config.NFS_RAW_ROOT_DIR, config.NFS_RAW_DST_DIR)
            try:
                res = self.init_ingestion_src_fs(src, dst)
                return res
            except FileNotFoundError as e:
                raise e
            except Exception as e1:
                raise Exception(f'Failed generating testing directory with error: {str(e1)}')

        else:
            raise ValueError(f'Illegal environment value type: {self.__env}')

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
        if self.__env == config.EnvironmentTypes.QA.name or self.__env == config.EnvironmentTypes.DEV.name:
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

        elif self.__env == config.EnvironmentTypes.PROD.name:
            try:
                file = os.path.join(self._get_folder_path_by_name(self.__shape), self.__shape_metadata_file)
                if config.FAILURE_FLAG:
                    source_name = self.__update_shape_fs_to_failure(file)
                else:
                    source_name = self.__update_shape_fs(file)
                _log.info(
                    f'[{file}]:was changed resource name: {source_name}')
            except Exception as e:
                _log.error(f'Failed on updating shape file: {file} with error: {str(e)}')
                raise e
            return source_name
        else:
            raise ValueError(f'Illegal environment value type: {self.__env}')

    def _change_max_zoom_level(self):
        """
        This will change max zoom level of ingestion -> by zoom level integer value in config
        :return: dict with results
        """
        if self.__env == config.EnvironmentTypes.QA.name or self.__env == config.EnvironmentTypes.DEV.name:
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

        elif self.__env == config.EnvironmentTypes.PROD.name:
            res = metadata_convertor.replace_discrete_resolution(self._dst_dir, str(config.zoom_level_dict[config.MAX_ZOOM_LEVEL]), 'tfw')
            return res

        else:
            raise ValueError(f'Illegal environment value type: {self.__env}')

    def init_ingestion_src_fs(self, src, dst):
        """
            This module will init new ingestion source folder - for file system / NFS deployment.
            The prerequisites must have source folder with suitable data and destination folder for new data
            The method will duplicate and rename metadata shape file to unique running name
            :return:dict with ingestion_dir and resource_name
        """
        if self.__watch:
            deletion_watch_dir = os.path.dirname(dst)
            if os.path.exists(deletion_watch_dir):
                command = f'rm -rf {deletion_watch_dir}/*'
                os.system(command)
        else:
            deletion_watch_dir = dst
            if os.path.exists(dst):
                _log.info(f'Will empty and clean old folder for test: {deletion_watch_dir}')
                command = f'rm -rf {deletion_watch_dir}'
                os.system(command)
                _log.info(f'Clear of [{deletion_watch_dir}] complete')
            else:
                _log.info(f'Directory [{deletion_watch_dir}] not exists, will generate new')

        if not os.path.exists(src):
            raise FileNotFoundError(f'[{src}] directory not found')

        try:
            _log.info(f'Start copy data for ingest from [{src}] to [{dst}]')
            command = f'cp -r {src}/. {dst}'
            os.system(command)
            if os.path.exists(dst):
                _log.info(f'Success copy and creation of test data on: {dst}')
            else:
                raise IOError('Failed on creating ingestion directory')

        except Exception as e2:
            _log.error(f'Failed copy files from {src} into {dst} with error: [{str(e2)}]')
            raise e2
        self._dst_dir = dst
        source_name = self._change_unique_sourceId()
        _log.info(f'SourceId (productId + product version) changed to: [{source_name}]')
        if config.PVC_UPDATE_ZOOM:
            _log.info(f"Requested for Max zoom limit for ingestion -> [{config.MAX_ZOOM_LEVEL}]")
            zoom_level = self._change_max_zoom_level()
            if not zoom_level[0]['success']:
                raise Exception(f'failed change zoom max level -> max resolution on tfw')
            else:
                _log.info(f'Change max zoom level: [{zoom_level[0]["reason"]}]')

        return {'ingestion_dir': self._dst_dir, 'resource_name': source_name}

    def validate_source_directory(self, path=None):
        """
        This will validate that source directory include all needed files for ingestion
        :param path: relative path of ingestion, None if working on azure - qa \ dev with pvc
        :return: True \ False + response json
        """
        if self.__env == config.EnvironmentTypes.QA.name or self.__env == config.EnvironmentTypes.DEV.name:
            resp = self.__pvc_handler.validate_ingestion_directory()
            try:
                content = json.loads(resp.text)
                if content.get('json_data'):
                    return not content['failure'], content['json_data']
                else:
                    return content['failure'], "missing json data"

            except Exception as e:
                raise Exception(f'Failed on getting response with error: {resp.status_code}|{resp.text}')

        elif self.__env == config.EnvironmentTypes.PROD.name:
            state, resp = self._validate_directory()
            content = resp.get('metadata')
            if content:
                validation_tiff_dict, error_msg = self._validate_tiff_exists(resp['fileNames'])
                assert all(
                    validation_tiff_dict.values()) is True, f' Failed: on following ingestion process [{error_msg}]'
                return True, content

            else:
                return False, "Failed on tiff validation"
        else:
            raise Exception(f'illegal Environment name: [{self.__env}]')

    def __update_shape_fs_to_failure(self, shp):
        resp = shape_convertor.add_ext_source_name(shp, 'duplication')
        return resp

    def __update_shape_fs(self, shp):
        current_time_str = common.generate_datatime_zulu().replace('-', '_').replace(':', '_')
        resp = shape_convertor.add_ext_source_name(shp, current_time_str)
        return resp

    def _validate_directory(self):
        """This module validate source directory is valid for ingestion process"""
        missing_set_files = []
        if not os.path.exists(self._dst_dir):
            _log.error(f'Path [{self._dst_dir}] not exists')
            return False, f'Path [{self._dst_dir}] not exists'

        if not self._find_if_folder_exists(self._dst_dir, self.__shape):
            _log.error(f'Path [{os.path.join(self._dst_dir, self.__shape)}] not exists')
            return False, f'Path [{os.path.join(self._dst_dir, self.__shape)}] not exists'

        if not self._find_if_folder_exists(self._dst_dir, self.__tif):
            _log.error(f'Path [{os.path.join(self._dst_dir, self.__tif)}] not exists')
            return False, f'Path [{os.path.join(self._dst_dir, self.__tif)}] not exists'

        ret_folder = self._get_folder_path_by_name(self.__shape)
        for file_name in cfg.files_names:
            for ext in cfg.files_extension_list:
                ret_extension_validation, missing = cfg.validate_ext_files_exists(
                    os.path.join(ret_folder, file_name), ext)
                if not ret_extension_validation:
                    missing_set_files.append(missing)
        if missing_set_files:
            return False, f'Path [{os.path.join(self._dst_dir, self.__shape)}] missing files:{set(missing_set_files)}'

        json_object_res = shape_functions.ShapeToJSON(ret_folder)

        return True, json_object_res.created_json

    def _find_if_folder_exists(self, directory, folder_to_check):
        os.walk(directory)
        directory_lists = [x[0] for x in os.walk(directory)]
        for current_dir in directory_lists:
            if folder_to_check in current_dir:
                return True
        return False

    def _get_folder_path_by_name(self, name):
        p_walker = [x[0] for x in os.walk(self._dst_dir)]
        path = ("\n".join(s for s in p_walker if name.lower() in s.lower()))
        return path

    def _validate_tiff_exists(self, tiff_list):
        err = ''
        x = {}
        text_files = glob.glob(self._dst_dir + "/**/*.tif", recursive=True)
        for item in tiff_list:
            if '.' in item:
                item = item.split('.')[0]
            if any(item in text for text in text_files):
                x[item] = True
            else:
                x[item] = False
                err = 'tiff files missing :' + item
        if len(text_files) != len(tiff_list):
            x['length_is_equal'] = False
            err = 'tiff files length is not equal'
        else:
            x['length_is_equal'] = True
        if err:
            return x, err
        else:
            return x, ""

    def count_tiles_on_storage(self, product_id, product_version, tiles_format='png'):
        """
        This method count for total amount of tiles images on storage FS | S3
        :param product_id: discrete resource id
        :param product_version: version of discrete
        :return: int -> number of
        """
        if config.STORAGE_TILES == "S3":
            _log.info(f'Collect total amount of tiles on S3 for layer: {product_id}-{product_version}\n'
                      f'For object_key: [{"/".join([product_id, product_version])}]')
            s3_conn = s3storage.S3Client(config.S3_ENDPOINT_URL, config.S3_ACCESS_KEY, config.S3_SECRET_KEY)
            object_key = "/".join([product_id, product_version])
            tiles_list = s3_conn.list_folder_content(config.S3_BUCKET_NAME, object_key)
            _log.info(f'Total tiles count: [{len(tiles_list)}]')
            return len(tiles_list)

        elif config.STORAGE_TILES == "FS":
            _log.info(f'Collect total amount of tiles on FS for layer: {product_id}-{product_version}\n'
                      f'For directory: [{os.path.join(config.NFS_RAW_ROOT_DIR, config.TILES_RELATIVE_PATH, product_id, product_version)}]')
            tiles_dir = os.path.join(config.NFS_RAW_ROOT_DIR, config.TILES_RELATIVE_PATH, product_id, product_version)
            tiles_list = glob.glob(tiles_dir + f"/**/*.{tiles_format}", recursive=True)
            _log.info(f'Total tiles count: [{len(tiles_list)}]')
            return len(tiles_list)
        else:
            raise Exception(f'illegal Tiles Storage provider name: [{config.STORAGE_TILES}]')