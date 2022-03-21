"""
This module will wrap all functionality interfacing with data on s3 (tiles)
"""
import logging
import json
from mc_automation_tools import s3storage
from mc_automation_tools.parse import stringy

_log = logging.getLogger('sync_tester.cleanup_cli.s3')


class S3Handler:

    def __init__(self, s3_credential):
        """
        Initial s3 connection object that contain deployment relevant credits to query and use CRUD operation
        :param s3_credential: dict [json] -> sample:
        s3_credential =  {
                  "s3_endpoint_url": string [ip],
                  "s3_bucket_name": string ,
                  "s3_access_key": string,
                  "s3_secret_key": string,
                  }
        """
        self._end_point_url = s3_credential.get('s3_endpoint_url')
        self._bucket = s3_credential.get('s3_bucket_name')
        self.__access_key = s3_credential.get('s3_access_key')
        self.__secret_key = s3_credential.get('s3_secret_key')

    @property
    def get_class_params(self):
        params = {
            'end_point_url': self._end_point_url,
            'bucket': self._bucket
        }
        return params

    def _submit_connection(self):
        """
        Helper method to create s3 connection client
        :return: s3 client
        """
        try:
            s3_conn = s3storage.S3Client(endpoint_url=self._end_point_url,
                                         aws_access_key_id=self.__access_key,
                                         aws_secret_access_key=self.__secret_key,
                                         )
            _log.debug(
                f'Connection to {self._end_point_url} created')
            return s3_conn

        except Exception as e:
            err = f'Failed Connect {self._end_point_url} with error: [{str(e)}'
            _log.error(err)
            raise ConnectionError(err)

    def is_object_exists(self, layer_name):
        """
        This method will check if provided layer is exists on s3 bucket
        :param layer_name: Full name of layer
        :return: bool
        """

        try:
            s3_conn = self._submit_connection()
            res = s3_conn.is_object_exists(bucket_name=self._bucket, path=layer_name)
            _log.debug(f'Layer: {layer_name} store with object key on S3, bucket name: {self._bucket} -> state: [{res}]')

        except Exception as e:
            err = f'Failed Connect/Get data with error: [{str(e)}'
            _log.error(err)
            raise ConnectionError(err)

        return res

    def list_object(self, layer_name):
        """
        This method will list all files & folders to provided layer name (object key)
        :param layer_name: Full name of layer
        :return: list
        """

        try:
            s3_conn = self._submit_connection()
            res = s3_conn.list_folder_content(bucket_name=self._bucket, directory_name=layer_name)
            return res

        except Exception as e:
            err = f'Failed Get data with error: [{str(e)}'
            _log.error(err)
            raise ConnectionError(err)

    def remove_layer_from_bucket(self, layer_name):
        """
        This method empty and remove object and is internal items from bucket
        :param layer_name:str -> represent the object key to be removed
        :return: dict -> {state: bool, msg: str, extra: dict -> response orig data}
        """

        try:
            s3_conn = self._submit_connection()
            res = s3_conn.delete_folder(self._bucket, layer_name)
            if isinstance(res, bool):
                return {'state': True, 'msg': 'Not exists', 'extra': res}
            s_code = res['ResponseMetadata']['HTTPStatusCode']
            if s_code != 200:
                _log.warning(f'Failed deletion for layer: [{layer_name}] with status code: [{s_code}]')
                return {'state': False, 'msg': s_code, 'extra': res}

            _log.info(f'Success of deletion:\n'
                      f'Bucket: {self._bucket}\n'
                      f'Object key: {layer_name}\n'
                      f'Status code: {s_code}\n'
                      f'Total deleted items: {len(res["Deleted"])}')
            return {'state': True, 'msg': s_code, 'extra': res}

        except Exception as e:
            err = f'Failed connect and access data with error: [{str(e)}'
            _log.error(err)
            raise ConnectionError(err)

    def clean_layer_from_s3(self, layer_name):
        """
        This method will validate exists of layer's tiles on s3 and remove tiles
        :param layer_name: represent the object key inside the configured bucket
        :return: dict
        """

        _log.info(
            "\n\n" + stringy.pad_with_stars(
                f'Start S3 tiles cleaning for layer: [{layer_name}]'))

        if not self.is_object_exists(layer_name):
            _log.info(f'Current object key not exists!')
            return {'state': False, 'msg': f'Current object key not exists! [{layer_name}]'}

        try:
            list_of_tiles = self.list_object(layer_name)

            _log.info(f'Bucket: [{self._bucket}] | Object key: [{layer_name}] include [{len(list_of_tiles)}] items\n'
                      f'To list all items in object, run on DEBUG log level...')
            _log.debug(
                f"Items that found:\n"
                f"{json.dumps(list_of_tiles, indent=1)}")

            _log.info(f'Will execute cleanup for {layer_name} object, contain: {len(list_of_tiles)} tiles')

            remove_results = self.remove_layer_from_bucket(layer_name)

            _log.info(f'Results status for deletion:\n'
                      f'Deletion complete: [{remove_results["state"]}]\n'
                      f'Result deletion state: [{remove_results["msg"]}]\n'
                      f'Result extra data: [{json.dumps(remove_results["extra"], indent=3)}]')

            return {'state': True, 'msg': f'Layer - {layer_name}, deleted {len(list_of_tiles)} items'}

        except Exception as e:
            _log.error(f'Failed execute deletion from S3 with error: {str(e)}')
            raise Exception(f'Failed execute deletion from S3 with error: {str(e)}')


