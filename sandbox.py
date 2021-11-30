from sync_tester.jobs_api_wrapper import jobs_tasks
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

jobs = jobs_tasks.JobsTasksManager('https://job-manager-qa-job-manager-route-raster.apps.v0h0bdx6.eastus.aroapp.io')
x = jobs.get_class_params
print(x)
res = jobs.tasks('0f6ad8f2-baac-4365-8edf-030a80c93dbb')[0]
body_test ={'attempts': 0, 'status': 'Completed', 'type': 'Discrete-Tiling', 'percentage': None, 'reason': 'testing_ronen', 'description': ''}
res = jobs.create_task('0f6ad8f2-baac-4365-8edf-030a80c93dbb', body_test)
res = jobs.resettable('0f6ad8f2-baac-4365-8edf-030a80c93dbb')
res = jobs.find_jobs_by_criteria(parameters)

res = jobs.get_job_by_id('4f309944-c6bc-4ab5-b7d0-faf95d56b6ff', return_tasks=False)

res = jobs.updates_job('0f6ad8f2-baac-4365-8edf-030a80c93dbb',update_body)
res = jobs.delete_job('0f6ad8f2-baac-4365-8edf-030a80c93dbb')
