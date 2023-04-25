import boto3
import json
import os

# To retrieve the secret, you need to allow the secretsmanager:GetSecretValue API call in your IAM policy.
# Alternatively, you can attach the SecretsManagerReadWrite policy to the user who needs permission to manage AWS Secrets Manager.

secret_configs = {
    "njoy-fms-qa": '''{
        
        "SF_ACCOUNT": "",
        "SF_USER": "",
        "SF_PASSWORD": "",
        "SF_ROLE":"",
        "SF_DATABASE": "",
        "SF_SCHEMA": "",
        "SF_WEARHOUSE": "",
        
        "JIRA_USER_ID":"",
        "JIRA_CREATE_ISSUE_API_ENDPOINT":"",
        "JIRA_ACCESS_TOKEN":""
                
    }''', 
    "njoy-fms-prod": '''{
        "SF_ACCOUNT": "",
        "SF_USER": "",
        "SF_PASSWORD": "",
        "SF_ROLE":"",
        "SF_DATABASE": "",
        "SF_SCHEMA": "",
        "SF_WEARHOUSE": "",
        
        "JIRA_USER_ID":"",
        "JIRA_CREATE_ISSUE_API_ENDPOINT":"",
        "JIRA_ACCESS_TOKEN":""
    }'''
}

client = boto3.client("secretsmanager")

def create_secret(secret_name, secret_string):
    return client.create_secret(
        Name=secret_name,
        SecretString=secret_string
    )
    
def get_all_secrets():
    return client.list_secrets()

def get_secret_from_id(secret_name): 
    response = client.get_secret_value(
        SecretId=secret_name
    )
    jsonified_secrets = json.loads(response["SecretString"])
    return jsonified_secrets

ACTIVE_PROFILE = os.getenv('FMS_ACTIVE_ENVIRONMENT') # Possible values: njoy-fms-qa / njoy-fms-prod

secret_string = secret_configs[ACTIVE_PROFILE]
resp = create_secret(secret_name=ACTIVE_PROFILE, secret_string=secret_string)
print(resp)
print("=============================================================================================")
if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
    print("=========== Secret with active profile named " + ACTIVE_PROFILE + " is Created Successfully ===========")
print(get_all_secrets())
print("=============================================================================================")
secret = get_secret_from_id(secret_name=ACTIVE_PROFILE)
print(secret)
print("=================================== Validation Successful ==================================")