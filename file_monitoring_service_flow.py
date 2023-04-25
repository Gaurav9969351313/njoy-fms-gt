import datetime
import json
import time
import jsonschema
import requests
import boto3
import pandas as pd
import snowflake.connector as sf
import sys

from datetime import date
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger, task
from snowflake.connector.pandas_tools import write_pandas 
from jsonschema import validate
import os

APP_CONFIG = {}
conn = {}
logger = {}

def get_env(var, default = None):
    return os.environ[var] or default

@task(name="AWS Secret Manager", description="Get Secrets From AWS Secret manager")
def get_secret_from_id(secret_name): 
    sm_client = boto3.client("secretsmanager")
    response = sm_client.get_secret_value(
        SecretId=secret_name
    )
    jsonified_secrets = json.loads(response["SecretString"])
    return jsonified_secrets

# def retrieve_jobs_configuration():
#     with open('jobs.prod.json') as f:
#         d = json.load(f)
#     return d    

def formatsize(num, suffix="B"):
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if num is not None and abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.2f}Yi {suffix}"

@task(name="Retrive Metadata from AWS S3", task_run_name="Location={bucket_name}/{folder_path}")
def list_s3_files_in_folder_using_client(bucket_name, folder_path, file_prefix, additional_filter_required, additional_filter_value):
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    files = response.get("Contents")
    
    # As it has subfolder we do require additional filter here.
    if additional_filter_required:
        filtered_files = []
        for file in files:
            if additional_filter_value in file['Key'].split('/'):
                pass
            else:
                filtered_files.append(file)
        files = filtered_files
        
    if file_prefix is not None:
        filtered_files_prefix = []
        for file in files:
            if file['Key'].startswith(folder_path + file_prefix):
                file['Size'] = formatsize(file['Size'])
                filtered_files_prefix.append(file)
        files = filtered_files_prefix
                
    if len(files) > 0:    
        last_uploaded_file = max(files, key=lambda x: x['LastModified'])
    else:
        last_uploaded_file = None
        
    files_df = pd.DataFrame(files)
    files_df.reset_index()
    return files_df, last_uploaded_file

def snowflake_initial_setup():
    try:
        warehouse_sql = 'use warehouse {}'.format(APP_CONFIG['SF_WEARHOUSE'])
        run_query(conn, warehouse_sql)
        
        try:
            sql = 'alter warehouse {} resume'.format(APP_CONFIG['SF_WEARHOUSE'])
            run_query(conn, sql)
        except:
            pass
        
        sql = 'use database {}'.format(APP_CONFIG['SF_DATABASE'])
        run_query(conn, sql)
        
        sql = 'use role {}'.format(APP_CONFIG['SF_ROLE'])
        run_query(conn, sql)
        
        sql = 'use schema {}'.format(APP_CONFIG['SF_SCHEMA'])
        run_query(conn, sql)
        get_run_logger().info("========== Snowflake Initiation Done.                 ==========")
    except Exception as e:
        print(e)

def run_query(connection,query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()  

def query_snowflake_to_get_df(sql):
    cur = conn.cursor()
    cur.execute(sql)
    while True:
        dat = cur.fetchmany(50000)
        if not dat:
            break
        df = pd.DataFrame(dat, columns=cur.description)
    cur.close()
    return df

def get_table_metadata(df):
    def map_dtypes(x):
        if (x == 'object') or (x=='category'):
            return 'VARCHAR'
        elif 'date' in x:
            return 'DATETIME'
        elif 'int' in x:
            return 'NUMERIC'  
        elif 'float' in x: return 'FLOAT' 
        else:
            print("cannot parse pandas dtype")
    sf_dtypes = [map_dtypes(str(s)) for s in df.dtypes]
    table_metadata = ", ". join([" ".join([y.upper(), x]) for x, y in zip(sf_dtypes, list(df.columns))])
    return table_metadata

def df_to_snowflake_table(table_name, operation, df, conn=conn): 
    if operation=='create_table_if_not_exists':
        df.columns = [c.upper() for c in df.columns]
        table_metadata = get_table_metadata(df)
        conn.cursor().execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_metadata})")
        write_pandas(conn, df, table_name.upper())
    elif operation=='append':
        table_rows = str(list(df.itertuples(index=False, name=None))).replace('[','').replace(']','')
        conn.cursor().execute(f"INSERT INTO {table_name} VALUES {table_rows}")  

def raise_jira_issue():
    url = APP_CONFIG['JIRA_CREATE_ISSUE_API_ENDPOINT']

    payload = json.dumps({
    "update": {},
    "fields": {
        "summary": "File Not Available " + str(datetime.datetime.now()),
        "issuetype": {
            "id": "10006"
        },
        "components": [{
                "id": "10067"
        }],
        "project": {
            "id": "10008"
        },
        "description": {
        "type": "doc",
        "version": 1,
        "content": [
            {
            "type": "paragraph",
            "content": [
                {
                "text": "File is not availbe in Nelson AWS s3 Bucket." + str(datetime.datetime.now()),
                "type": "text"
                }
            ]}
        ]},
        "reporter": {
            "id": APP_CONFIG['JIRA_USER_ID']
        },
        "priority": {
            "id": "2"
        },
        "labels": [
            "fms_file_not_available"
        ]}
    })
    
    headers = {
        'Authorization': 'Basic ' + APP_CONFIG['JIRA_ACCESS_TOKEN'],
        'Content-Type': 'application/json'
    }
    try: 
        print("================= JIRA Request Built.              =================")
        response = requests.request("POST", url, headers=headers, data=payload)
        jira_data_obj = json.loads(response.text)
        print(jira_data_obj)
        print("================= JIRA Ticket Raised Successfully. =================")
        return jira_data_obj
    except: 
        print("************** Error Connecting With Jira API **************")

def common_init():
    global logger
    logger = get_run_logger()
    
    global APP_CONFIG
    
    environment = os.environ['FMS_ACTIVE_ENVIRONMENT']
    
    if environment == None:
        get_run_logger().error("Environment Is Not Set. Please Set an Environment Variable named FMS_ACTIVE_ENVIRONMENT. ") 
        sys.exit()   
 
    if environment == 'ic-dev' or environment == 'njoy-fms-dev':
        APP_CONFIG['SF_ACCOUNT'] = get_env('SF_ACCOUNT')
        APP_CONFIG['SF_USER'] = get_env('SF_USER')
        APP_CONFIG['SF_PASSWORD'] = get_env('SF_PASSWORD') 
        APP_CONFIG['SF_DATABASE'] = get_env('SF_DATABASE')
        APP_CONFIG['SF_SCHEMA'] = get_env('SF_SCHEMA')
        APP_CONFIG['SF_WEARHOUSE'] = get_env('SF_WEARHOUSE')
        APP_CONFIG['SF_ROLE'] = get_env('SF_ROLE')
        
        APP_CONFIG['JIRA_USER_ID'] = get_env('JIRA_USER_ID')
        APP_CONFIG['JIRA_CREATE_ISSUE_API_ENDPOINT'] = get_env('JIRA_CREATE_ISSUE_API_ENDPOINT')
        APP_CONFIG['JIRA_ACCESS_TOKEN'] = get_env('JIRA_ACCESS_TOKEN')
        
    if environment == 'njoy-fms-qa':
        APP_CONFIG = get_secret_from_id('fms-qa-environment')
    if environment == 'njoy-fms-prod':
        APP_CONFIG = get_secret_from_id('fms-prod-environment')
    
    global conn
    
    try:
        conn = sf.connect(
                account = APP_CONFIG['SF_ACCOUNT'],
                user = APP_CONFIG['SF_USER'],
                password = APP_CONFIG['SF_PASSWORD'],
                ocsp_fail_open=False
        )
    except:
        get_run_logger().error("************* Error Occured While Obtaining Snowflake Connection *************")

    snowflake_initial_setup()

def get_valid_job_schema():
    return {
                "$schema":"http://json-schema.org/draft-04/schema#",
                "title":"Job",
                "description":"A Job Configuration Json",
                "type":"object",
                "properties":{
                    "id":{
                        "description":"The unique identifier for a job",
                        "type":"integer"
                    },
                    "job_name": {
                        "description":"Job Name",
                        "type":"string"
                    },
                    "description": {
                        "description":"Job Description",
                        "type":"string"
                    },
                    "workflow_to_invoke": {
                        "description": "AWS_S3_FILE_CHECK as Default",
                        "type": "string"
                    },
                    "bucket_location": {
                        "description":"AWS S3 Bucket Name",
                        "type":"string"
                    },
                    "folder_location": {
                        "description":"Folder Location to scan for new file arrival",
                        "type":"string"
                    },
                    "file_prefix": {
                        "description":"File name pattern that we expect to come",
                        "type":"string"
                    },
                    "additional_filter_required": {
                        "description": "If their is a nested folder structure then keep it 1 else 0",
                        "type":"integer"
                    },
                    "additional_filter_value": {
                        "description": "Nested folder name to skip",
                        "type":"string"
                    },
                    "expected_new_file_count": {
                        "description": "Exact difference we expect as a new file upload in count",
                        "type":"integer"
                    },
                    "scan_type_category": {
                        "description":"Job Schedule Category. Possible Values are HOURLY/DAILY/WEEKLY/MONTHLY",
                        "type":"string"
                    },
                    "cron_expression": {
                        "description":"Cron Expression String For Sceduling",
                        "type":"string"
                    }
                }
            }

@flow(name="FMS-INIT")
def fms_initial_setup(job_configs):
    print("========= FMS Initial Setup Called =========")
    common_init()
    logger_df = pd.DataFrame(columns=['job_id', 'job_name', 'workflow_invoked', 'timestamp', 'event_name', 'filename', 'filesize'])
    df_to_snowflake_table('tbl_njoy_file_logger', 'create_table_if_not_exists', logger_df, conn=conn)
    
    run_query(conn, 'truncate table raw.rawstage.tbl_temp_storage;')
    time.sleep(3)
    for job in job_configs:
        df_from_AWS_S3, last_modified_file = list_s3_files_in_folder_using_client(
                                                                        bucket_name=job['bucket_location'], 
                                                                        folder_path=job['folder_location'],
                                                                        file_prefix=job['file_prefix'],
                                                                        additional_filter_required=job['additional_filter_required'],
                                                                        additional_filter_value=job['additional_filter_value'])
        df_from_AWS_S3.insert(0, 'job_name', job['job_name'])
        df_to_snowflake_table('tbl_temp_storage', 'create_table_if_not_exists', df_from_AWS_S3, conn=conn)
        get_run_logger().warn("========== Reference Data for Job Id [" + str(job['id']) + '] & Location = ' + job['bucket_location'] + '/' + job['folder_location'] + '/' + job['file_prefix'] + " Inserted Sucessfully. ==========")
        time.sleep(2)
        
@flow(name="file-monitoring-service")
def start_file_monitoring_service(job): 
    get_run_logger().info("======================================= FMS :- Main Function Started =======================================") 
    # Validatiing incoming JSON job Configuration.
    try:
        get_run_logger().info(job)
        validate(instance=job, schema=get_valid_job_schema())
        get_run_logger().info("==== JOB Configuration Validation Successful ====")
    except jsonschema.exceptions.ValidationError as e:
        get_run_logger().error("well-formed but invalid JSON:", e)
    except json.decoder.JSONDecodeError as e:
        get_run_logger().error("poorly-formed text, and it is not JSON:", e)
        
    common_init()
    # for job in job_configurations:
    get_run_logger().info("Job Name :-           " + job['job_name'])
    get_run_logger().info("Scan Type :-          " + job['scan_type_category'])
    get_run_logger().info("Workflow To Invoke :- " + job['workflow_to_invoke'])
    get_run_logger().info("Bucket Name :-        " + job['bucket_location'])
    get_run_logger().info("Folder Name :-        " + job['folder_location'])
    get_run_logger().info("File Prefix :-        " + job['file_prefix'])
    get_run_logger().info("Description :-        " + job['description'])
    get_run_logger().info("=================================================")
    
    df_from_AWS_S3, last_modified_file = list_s3_files_in_folder_using_client(
                                                            bucket_name=job['bucket_location'], 
                                                            folder_path=job['folder_location'],
                                                            file_prefix=job['file_prefix'],
                                                            additional_filter_required=job['additional_filter_required'],
                                                            additional_filter_value=job['additional_filter_value'])
    
    df_database = query_snowflake_to_get_df("select * from tbl_temp_storage where job_name = '" + job['job_name']+ "'");
    df_database.columns = ['job_name','Key', 'LastModified', 'ETag', 'Size', 'StorageClass']
    
    # =============================== Testing Starts Here =============================== 
    # df_from_AWS_S3.loc[len(df_from_AWS_S3.index)] = ['transfers/nielsen/Njoy_Weekly_fct_333333.txt.gz', '2023-03-14 07:00:05+00:00', '180311567e3ebbb211d1259f19b81a21-218', '1.5GB', 'MANUALLY_ADDED'] 
    # =============================== Testing Ends Here ===============================
    
    difference_df = df_from_AWS_S3.merge(df_database, on = 'Key', indicator = True, how='left').loc[lambda x : x['_merge']!='both']  
    log_obj = {}     
    print("difference_df count(rows):- " + str(difference_df.shape[0]))    
           
    if difference_df.empty:
        log_obj['job_id'] = job['id']
        log_obj['job_name'] = job['job_name']
        log_obj['workflow_to_invoke'] = job['workflow_to_invoke'] 
        log_obj['timestamp'] = str(datetime.datetime.now())
        log_obj['event_name'] = "FILE_NOT_AVAILABLE"
        log_obj['filename'] = ""
        log_obj['filesize'] = ""
        log_obj_data = {k:[v] for k,v in log_obj.items()} 
        df_log = pd.DataFrame(log_obj_data)
        df_log.columns = ['job_id','job_name','workflow_to_invoke','timestamp','event_name','filename','filesize']
        df_to_snowflake_table('tbl_njoy_file_logger', 'append', df_log, conn=conn)
        jira_response = raise_jira_issue()
        get_run_logger().error("========== No New File Upload is found ==========")
        get_run_logger().info("========== Sending Jira Notification.  ==========")
        
    elif difference_df.shape[0] >= job['expected_new_file_count']: 
        get_run_logger().warn("======================== New File Found ========================")
        df_to_append =  difference_df[['Key', 'LastModified_x', 'ETag_x', 'Size_x', 'StorageClass_x']]
        df_to_append.columns = ['Key', 'LastModified', 'ETag', 'Size', 'StorageClass']
        get_run_logger().info("Job Name      :- " + job['job_name'])
        get_run_logger().info("Key           :- " + df_to_append.iloc[0]['Key'])
        get_run_logger().info("Last Modified :- " + str(df_to_append.iloc[0]['LastModified']))
        get_run_logger().info("Size          :- " + str(df_to_append.iloc[0]['Size']))
        get_run_logger().info("================================================================")
        df_to_append.insert(0, 'job_name', job['job_name'])
        df_to_append['LastModified'] = str(df_to_append.iloc[0]['LastModified'])
        
        df_to_snowflake_table('tbl_temp_storage', 'append', df_to_append, conn=conn)
        
        log_obj['job_id'] = job['id']
        log_obj['job_name'] = job['job_name']
        log_obj['workflow_to_invoke'] = job['workflow_to_invoke'] 
        log_obj['timestamp'] = str(datetime.datetime.now())
        log_obj['event_name'] = "NEW_FILE_FOUND"
        log_obj['filename'] = df_to_append.iloc[0]['Key']
        log_obj['filesize'] = str(df_to_append.iloc[0]['Size'])
        log_obj_data = {k:[v] for k,v in log_obj.items()} 
        df_log = pd.DataFrame(log_obj_data)
        df_log.columns = ['job_id','job_name','workflow_to_invoke','timestamp','event_name','filename','filesize']
        df_to_snowflake_table('tbl_njoy_file_logger', 'append', df_log, conn=conn)
    
    get_run_logger().info("======================================= FMS :- Main Function End =======================================")

# if __name__== "__main__":
#     if len(sys.argv) > 1 and sys.argv[1] == "INIT":
#         fms_initial_setup(job_configs={})
#     else:
#         start_file_monitoring_service(job={})
