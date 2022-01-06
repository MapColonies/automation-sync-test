"""
This module wrapping and provide easy access client for ingestion-sync functionality on postgres
"""
import logging
from sync_tester.configuration import config
from mc_automation_tools import postgres

_log = logging.getLogger('sync_tester.postgres.postgres_adapter')


class PostgresHandler:
    __job_task_db = config.PG_JOB_TASK_DB_CORE_A
    __pycsw_records_db = config.PG_PYCSW_RECORD_DB_CORE_A
    __mapproxy_config_db = config.PG_MAPPROXY_DB_CORE_A
    __agent_db = config.PG_AGENT_DB_CORE_A
    __mapproxy_config_table = 'config'

    def __init__(self, end_point_url):
        self.__end_point_url = end_point_url
        self.__user = config.PG_USER_CORE_A
        self.__password = config.PG_PASS_CORE_A


    @property
    def get_class_params(self):
        params = {
            'end_point_url': self.__end_point_url,
            'job_task_db': self.__job_task_db,
            'pycsw_records_db': self.__pycsw_records_db,
            'mapproxy_config_db': self.__mapproxy_config_db,
            'agent_db': self.__agent_db,
            'mapproxy_config_table': self.__mapproxy_config_table
        }
        return params

    # ============================================== jobs & task =======================================================

    def get_current_job_id(self, product_id, product_version):
        """
        This query return the latest job id, based on creationTime on db, user provide product id and product version
        :param product_id: resource id
        :param product_version: layer version
        :return: str [job id[
        """
        client = postgres.PGClass(self.__end_point_url, self.__job_task_db, self.__user, self.__password)
        keys_values = {'resourceId': product_id, 'version': product_version}
        res = client.get_rows_by_keys('Job', keys_values, order_key='creationTime', order_desc=True)
        latest_job_id = res[0][0]
        _log.info(f'Received current job id: [{latest_job_id}], from date: {res[0][6]}')
        return latest_job_id

    def get_job_by_id(self, job_id):
        """
        This method return job by providing id (jobId)
        :param job_id: id of relevant job
        :return: dict of job data
        """
        client = postgres.PGClass(self.__end_point_url, self.__job_task_db, self.__user, self.__password)
        res = client.get_rows_by_keys('Job', {'id': job_id}, return_as_dict=True)
        return res[0]

    def get_tasks_by_job(self, job_id):
        """
        This method return list of tasks [raws] by provide job id [jobId]
        :param job_id: id of relevant job
        :return: dict of job data
        """
        client = postgres.PGClass(self.__end_point_url, self.__job_task_db, self.__user, self.__password)
        res = client.get_rows_by_keys('Task', {'jobId': job_id}, return_as_dict=True)
        return res

    def clean_layer_history(self, job_id):
        """
        This method will delete record of job on agent db -> in case user want ingest same layer again from watch dir
        :param job_id:id of relevant job
        :return:
        """
        deletion_command = f"""DELETE FROM "layer_history" WHERE "layerId"='{job_id}';"""
        client = postgres.PGClass(self.__end_point_url, self.__agent_db, self.__user, self.__password)
        try:
            client.command_execute([deletion_command])
            _log.info(f'Cleaned up successfully (layer_history) - from [{self.__agent_db}] , job: [{job_id}]')
            return {'status': "OK", 'message': f'deleted ok {job_id}'}

        except Exception as e:
            return {'status': "Failed", 'message': f'deleted Failed: [{str(e)}]'}

    def clean_job_task(self, job_id):
        """
        This method will delete record of job on job task db [job manager db] -> deletion of job on Jobs table
        and related tasks on Tasks table
        :param job_id: id of relevant job
        :return: dict -> {'status': Bool, 'message': str'}
        """
        deletion_command = f"""DELETE FROM "Task" WHERE "jobId"='{job_id}';DELETE FROM "Job" WHERE "id"='{job_id}';"""
        client = postgres.PGClass(self.__end_point_url, self.__job_task_db, self.__user, self.__password)
        try:
            client.command_execute([deletion_command])
            _log.info(f'Cleaned up successfully (job + task)- [{self.__job_task_db}] job: [{job_id}]')
            return {'status': "OK", 'message': f'deleted ok {job_id}'}

        except Exception as e:
            return {'status': "Failed", 'message': f'deleted Failed: [{str(e)}]'}

    # ========================================== catalog - pycsw =======================================================

    def clean_pycsw_record(self, product_id):
        """
        This method will delete record of layer on pycsw db -> its unique record
        :param product_id: layer id -> resourceId
        :return: dict -> {'status': Bool, 'message': str'}
        """
        deletion_command = f"""DELETE FROM "records" WHERE "product_id"='{product_id}'"""
        client = postgres.PGClass(self.__end_point_url, self.__pycsw_records_db, self.__user, self.__password)
        try:
            client.command_execute([deletion_command])
            _log.info(
                f'Cleaned up successfully (record pycsw) - from [{self.__pycsw_records_db}] , records: [{product_id}]')
            return {'status': "OK", 'message': f'deleted ok {product_id}'}

        except Exception as e:
            return {'status': "Failed", 'message': f'deleted Failed: [{str(e)}]'}

    # =============================================== mapproxy =========================================================

    def get_mapproxy_config(self):
        """
        This method will return current configuration of layer on mapproxy config db
        :return: dict -> json
        """
        client = postgres.PGClass(self.__end_point_url, self.__mapproxy_config_db, self.__user, self.__password)
        try:
            res = client.get_column_by_name(table_name='config', column_name="data")[0]
            _log.info(f'got json-config ok')
            return {'status': "OK", 'message': res}

        except Exception as e:
            return {'status': "Failed", 'message': f'Failed get json-config: [{str(e)}]'}

    def get_mapproxy_configs(self):
        """
        This will return all mapproxy configuration exists by last creation chronology
        :return: list of dicts
        """
        client = postgres.PGClass(self.__end_point_url, self.__mapproxy_config_db, self.__user, self.__password)
        res = client.get_rows_by_order(table_name=self.__mapproxy_config_table, order_key='updated_time', order_desc=True,
                                       return_as_dict=True)
        _log.info(f'Received {len(res)} of mapproxy config files')
        return res

    def delete_config_mapproxy(self, id, value):
        """
        This method will delete entire row on mapproxy
        :param id: id of specific configuration
        :param value: layer id on mapproxy config
        :param value: dict -> json
        """
        client = postgres.PGClass(self.__end_point_url, self.__mapproxy_config_db, self.__user, self.__password)

        try:
            res = client.delete_row_by_id(self.__mapproxy_config_table, id, value)
            _log.info(f'delete mapproxy config with id [{id}] successfully')
            return {'status': "OK", 'message': res}

        except Exception as e:
            return {'status': "Failed", 'message': f'Failed on deletion json-config: [{str(e)}]'}


def delete_pycsw_record(product_id, value, db_name=config.PG_PYCSW_RECORD_DB_CORE_A, table_name='records'):
    """
    This method will delete entire row on mapproxy
    :param product_id: id of layer as product_id
    :param value: The product_id
    :param db_name: name of db
    :param table_name: name of table
    """
    client = postgres.PGClass(config.PG_HOST, db_name, config.PG_USER_CORE_A, config.PG_PASS_CORE_A)
    res = client.delete_row_by_id(table_name, product_id, value)


def delete_agent_path(layer_id, value, db_name=config.PG_AGENT_DB_CORE_A, table_name='layer_history'):
    """
    This method will delete entire row on mapproxy
    :param layer_id: represent the later unique ID
    :param value: value of id
    :param db_name: name of db
    :param table_name: name of table
    """
    client = postgres.PGClass(config.PG_HOST, db_name, config.PG_USER_CORE_A, config.PG_PASS_CORE_A)
    res = client.delete_row_by_id(table_name, layer_id, value)
