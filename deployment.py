import json
import sys
import os
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from file_monitoring_service_flow import start_file_monitoring_service, fms_initial_setup

def retrieve_jobs_configuration():
    environment = os.environ['FMS_ACTIVE_ENVIRONMENT']
    if environment == None:
        if environment == 'ic-dev' or environment == 'njoy-fms-dev':
            with open('jobs.dev.json') as f:
                d = json.load(f)
        if environment == 'njoy-fms-qa':
            with open('jobs.qa.json') as f:
                d = json.load(f)
        if environment == 'njoy-fms-prod':
            with open('jobs.prod.json') as f:
                d = json.load(f)
    return d 

def deploy_init_flow():
    job_configs = retrieve_jobs_configuration()
    Deployment.build_from_flow(
        version = "1.0",
        flow = fms_initial_setup,
        name = "FMS-INIT-JOB",
        parameters={'job_configs': job_configs },
        tags=["FMS-INIT"],
        work_queue_name="FMS-Test",
        apply=True
    )

def deploy_job_flows():
    job_configs = retrieve_jobs_configuration()
    for job in job_configs:
        Deployment.build_from_flow(
            version="1.0." + str(job['id']),
            flow=start_file_monitoring_service,
            name="FMS :: " + job['job_name'] + ' :: '  + str(job['scan_type_category']) + ' :: ' + str(job['id']),
            tags=["FMS-main"],
            parameters={ 'job': job},
            work_queue_name="FMS-Test",
            schedule=(CronSchedule(cron=job['cron_expression'])),
            apply=True
        )

if __name__== "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "INIT":
        deploy_init_flow()
    else:
        deploy_job_flows()