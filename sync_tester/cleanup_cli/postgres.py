"""
This module will wrap all functionality interfacing with data on DB (postgres)
"""
import json
import logging
import copy
from mc_automation_tools import postgres
from mc_automation_tools.parse import stringy

_log = logging.getLogger('sync_tester.cleanup_cli.postgres')


class PostgresHandler:

    def __init__(self, pg_credential):
        """
        Initial postgres connection object that contain deployment relevant credits to query and use CRUD operation
        :param pg_credential: dict [json] -> sample:
        pg_credential =  {
                  "pg_endpoint_url": string [ip],
                  "pg_port": int [default is 5432],
                  "pg_user": string,
                  "pg_pass": string,
                  "pg_db": {
                    "name": string,
                    "schemes": {
                      "discrete_agent": {
                        "name": string,
                        "tables": {
                          "layer_history": string
                        }
                      },
                      "job_manager": {
                        "name": string,
                        "tables": {
                          "job": string,
                          "task": string
                        }
                      },
                      "layer_spec": {
                        "name": string,
                        "tables": {
                          "tiles_counter": string
                        },
                        "indexes": {
                          "tiles_counter_id_seq": string
                        }
                      },
                      "mapproxy_config": {
                        "name": string,
                        "tables": {
                          "config": string
                        },
                        "indexes": {
                          "config_id_seq": string
                        }
                      },
                      "raster_catalog_manager": {
                        "name": string,
                        "tables": {
                          "records": string
                        }
                      }
                    }
                  }
                }
        """
        self.__end_point_url = pg_credential.get('pg_endpoint_url')
        self.__port = pg_credential.get('pg_port')
        self.__user = pg_credential.get('pg_user')
        self.__password = pg_credential.get('pg_pass')
        self.__db_name = pg_credential.get('pg_db').get('name')

        self.__discrete_agent_scheme = pg_credential.get('pg_db')['schemes']['discrete_agent']['name']
        self.__discrete_agent_table = pg_credential.get('pg_db')['schemes']['discrete_agent']['tables']['layer_history']

        self.__job_manager_scheme = pg_credential.get('pg_db')['schemes']['job_manager']['name']
        self.__job_manager_jobs_table = pg_credential.get('pg_db')['schemes']['job_manager']['tables']['job']
        self.__job_manager_tasks_table = pg_credential.get('pg_db')['schemes']['job_manager']['tables']['task']

        self.__layer_spec_scheme = pg_credential.get('pg_db')['schemes']['layer_spec']['name']
        self.__tiles_counter_table = pg_credential.get('pg_db')['schemes']['layer_spec']['tables']['tiles_counter']
        self.__tiles_counter_index = pg_credential.get('pg_db')['schemes']['layer_spec']['indexes'][
            'tiles_counter_id_seq']

        self.__mapproxy_scheme = pg_credential.get('pg_db')['schemes']['mapproxy_config']['name']
        self.__mapproxy_config_table = pg_credential.get('pg_db')['schemes']['mapproxy_config']['tables']['config']
        self.__mapproxy_config_index = pg_credential.get('pg_db')['schemes']['mapproxy_config']['indexes'][
            'config_id_seq']

        self.__catalog_manager_scheme = pg_credential.get('pg_db')['schemes']['raster_catalog_manager']['name']
        self.__catalog_records_table = pg_credential.get('pg_db')['schemes']['raster_catalog_manager']['tables'][
            'records']

    @property
    def get_class_params(self):
        params = {
            'end_point_url': self.__end_point_url,
            'port': self.__port,
            'db_name': self.__db_name,
            'schemes': {
                'discrete_agent': {
                    'name': self.__discrete_agent_scheme,
                    'discrete_agent_table_name': self.__discrete_agent_table
                },
                'job_manager': {
                    'name': self.__job_manager_scheme,
                    'jobs_table': self.__job_manager_jobs_table,
                    'tasks_table': self.__job_manager_tasks_table
                },
                'layer_spec': {
                    'name': self.__layer_spec_scheme,
                    'tiles_counter_table': self.__tiles_counter_table,
                    'tiles_counter_index': self.__tiles_counter_index
                },
                'mapproxy': {
                    'name': self.__mapproxy_scheme,
                    'mapproxy_config_table': self.__mapproxy_config_table,
                    'mapproxy_config_index': self.__mapproxy_config_index
                }
            }
        }
        return params

    # =========================================== read operation =======================================================
    def _get_connection_to_scheme(self, scheme_name):
        """
        This method create simple connection to specific scheme and return conn object
        """
        pg_conn = postgres.PGClass(host=self.__end_point_url,
                                   database=self.__db_name,
                                   user=self.__user,
                                   password=self.__password,
                                   scheme=scheme_name,
                                   port=self.__port)
        return pg_conn

    # =========================================== job manager ==========================================================

    def _get_jobs_by_criteria(self, product_id, product_version, product_type=None):
        """
        This method will return all jobs id by layer's criteria data
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param product_type: string [layer's type] -> optional, if not mentioned, will return all related jobs
        :return: list[str]
        """
        pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)
        criteria = {
            'resourceId': product_id,
            'version': product_version
        }
        if product_type:
            criteria['productType'] = product_type
        job_ids = pg_conn.get_rows_by_keys(table_name=self.__job_manager_jobs_table,
                                           keys_values=criteria,
                                           return_as_dict=True
                                           )
        return job_ids

    def _get_tasks_by_id(self, job_id):
        """
        This method will return all jobs id by layer's criteria data
        :param job_id: list [job_id]
        :return: list[str]
        """
        pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)

        task_ids = pg_conn.get_by_n_argument(table_name=self.__job_manager_tasks_table,
                                             pk='jobId',
                                             pk_values=copy.deepcopy(job_id),
                                             column='id'
                                             )
        return task_ids

    def _delete_tasks_by_job_id(self, jobs):
        """
        This method will execute deletion query on task table according list of job ids
        :param jobs: list[str]
        :return:
        """
        results = []
        for job in jobs:
            pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)
            resp = pg_conn.delete_row_by_id(self.__job_manager_tasks_table, "jobId", job)
            results.append(resp)
        return results

    def _delete_job_by_id(self, jobs):
        results = []
        for job in jobs:
            pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)
            resp = pg_conn.delete_row_by_id(self.__job_manager_jobs_table, "id", job)
            results.append(resp)
        return results

    def delete_job_task_by_layer(self, product_id, product_version, product_type=None):
        """
        This method will execute clean on layer's data in job_manager db's tables
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param product_type: string [layer's type] -> optional, if not mentioned, will remove all related jobs-tasks
        :return: dict => {state: bool, msg: dict}
        """

        _log.info(
            "\n\n" + stringy.pad_with_stars(
                f'Start Job manager DB cleaning for layer: [{product_id}:{product_version}]'))
        try:
            # collect id's for job and task to delete
            job_ids = self._get_jobs_by_criteria(product_id, product_version, product_type)
            if not job_ids:
                _log.info(f'No jobs found for layer {product_id}:{product_version}')
                report = {'state': False, 'msg': f'No jobs found for layer {product_id}:{product_version}'}
            else:
                job_ids = [job_id['id'] for job_id in job_ids]
                _log.info(f'Found {len(job_ids)} jobs to delete')
                # collect all relevant id of tasks related to job list
                task_ids = self._get_tasks_by_id(job_ids)
                _log.info(f'Found {len(task_ids)} tasks to delete')
                # execute deletion
                task_deletion_result = self._delete_tasks_by_job_id(job_ids)
                job_deletion_result = self._delete_job_by_id(job_ids)

                report = {'state': True, 'msg': {
                    'job_to_delete': len(job_ids),
                    'task_to_delete': len(task_ids),
                    'deleted_job': job_deletion_result,
                    'deleted_task': task_deletion_result
                }}
                _log.info(f'\n delete results: [{json.dumps(report, indent=4)}]')

        except Exception as e:
            _log.error(f'Failed execute deletion on job manager db with error: [{str(e)}]')
            report = {'state': False, 'msg': f'Failed execute deletion on job manager db with error: [{str(e)}]'}
        _log.info('\n' + stringy.pad_with_minus('End of Job manager DB deletion') + '\n')
        return report

    # ============================================ layer spec ==========================================================
    def get_tile_counter_rows(self, layer_id, target=None):
        """
        This method will query and find all records of tile counter (layer spec) for provided layer id
        :param layer_id: string => [product_id-product_version]
        :param target: string => default None and will search for all targets
        :return: list[dict] => records
        """
        pg_conn = self._get_connection_to_scheme(self.__layer_spec_scheme)
        criteria = {
            'layerId': layer_id
        }
        if target:
            criteria['target'] = target

        tiles_counter_records = pg_conn.get_rows_by_keys(table_name=self.__tiles_counter_table,
                                                         keys_values=criteria,
                                                         return_as_dict=True
                                                         )

        return tiles_counter_records

    def _delete_tile_counter_by_layer(self, layer_id):
        pg_conn = self._get_connection_to_scheme(self.__layer_spec_scheme)
        results = pg_conn.delete_row_by_id(self.__tiles_counter_table, "layerId", layer_id)
        return results

    def delete_tile_counter_by_layer(self, product_id, product_version, target=None):
        """
        This method will execute clean on layers spec db and remove related tile count for provided layer
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param target: string => default is None and will delete all layer's records
        :return: dict => {state: bool, msg: dict}
        """
        _log.info(
            "\n\n" + stringy.pad_with_stars(
                f'Start Job manager DB cleaning for layer: [{product_id}-{product_version}]'))
        layer_id = "-".join([product_id, product_version])

        try:
            tiles_counter_records = self.get_tile_counter_rows(layer_id=layer_id, target=target)
            if not tiles_counter_records:
                _log.info(f'Records not found for layer: [{layer_id}]')
                report = {'state': False, 'msg': f'Records not found for layer: [{layer_id}]'}
            else:
                _log.info(f'Found {len(tiles_counter_records)} records to delete:\n'
                          f'{json.dumps(tiles_counter_records, indent=4)}')
                resp = self._delete_tile_counter_by_layer(layer_id)
                _log.info(f'Records deletion were executed with state: {resp}')
                report = {'state': True, 'msg': resp}

        except Exception as e:
            _log.error(f'Failed tile counter DB clean up with error: [{str(e)}')
            report = {'state': False, 'msg': f'Failed tile counter DB clean up with error: [{str(e)}'}

        _log.info('\n' + stringy.pad_with_minus('End of layer spec - tile counter DB deletion') + '\n')
        return report


