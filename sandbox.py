from sync_tester.configuration import config
# from mc_automation_tools.postgres import agent_api
from sync_tester.postgres import postgres_adapter
from mc_automation_tools.sync_api import layer_spec_api as lsa

lsa_client = lsa.LayerSpec(config.LAYER_SPEC_ROUTE)
lsa_client.get_class_params
lsa_client.get_tiles_count('2021_12_27T12_34_25Z_MAS_6_ORT_247557-4.0', 'target2')
lsa_client.updates_tile_count('2021_12_27T12_34_25Z_MAS_6_ORT_247557-4.0', 'target2', {"tilesBatchCount": 5})
pa = postgres_adapter.PostgresHandler(config.PG_ENDPOINT_URL)
a = pa.get_mapproxy_config()
a = pa.get_mapproxy_configs()

print(a)
parameters = {
    'resourceId': '2021_11_16T16_09_23Z_MAS_6_ORT_247557',
    'version': '4.0',
    'isCleaned': False
}

body = {
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

update_body = {
    "status": "Failed",
    "percentage": 99,
    "reason": "test_update123",
    "isCleaned": True
}

# jobs = jobs_tasks.JobsTasksManager('https://job-manager-qa-job-manager-route-raster.apps.v0h0bdx6.eastus.aroapp.io')
# x = jobs.get_class_params
# print(x)
params_update_task = {
    "description": "blabla_testing",
    "status": "Failed",
    "percentage": 89,
    "reason": "testing update qa",
    "attempts": 2
}

params_find = {

      "status": "Failed",
}



from sync_tester.configuration import config
# jobs.update_expired_status()
# jobs.release_inactive()
# res = jobs.find_tasks(params_find)
# jobs.get_all_tasks_status('1d539e17-508f-41b4-8cfe-72b9afed7332')
# jobs.delete_task_by_task_id('0faa3071-4ae6-4023-af17-412f8a01600f', '0177b8bc-6f93-4602-a64c-b4dc69a7e76d')
# jobs.update_task_by_task_id('1d539e17-508f-41b4-8cfe-72b9afed7332', '05860f8a-b4cb-4f65-8442-27b41b360597',params_update_task)
# res = jobs.tasks('0f6ad8f2-baac-4365-8edf-030a80c93dbb')[0]
# body_test = {'attempts': 0, 'status': 'Completed', 'type': 'Discrete-Tiling', 'percentage': None,
#              'reason': 'testing_ronen', 'description': ''}
# res = jobs.create_task('0f6ad8f2-baac-4365-8edf-030a80c93dbb', body_test)
# res = jobs.resettable('0f6ad8f2-baac-4365-8edf-030a80c93dbb')
# res = jobs.find_jobs_by_criteria(parameters)
#
# res = jobs.get_job_by_id('4f309944-c6bc-4ab5-b7d0-faf95d56b6ff', return_tasks=False)
#
# res = jobs.updates_job('0f6ad8f2-baac-4365-8edf-030a80c93dbb', update_body)
# res = jobs.delete_job('0f6ad8f2-baac-4365-8edf-030a80c93dbb')
