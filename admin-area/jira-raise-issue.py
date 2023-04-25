import json
import requests

def raise_jira_issue():
    url = "https://njoyservicedesk.atlassian.net/rest/api/3/issue?updateHistory=false"

    payload = json.dumps({
    "update": {},
    "fields": {
        "summary": "File Not Available ",
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
                "text": "File is not availbe in Nelson AWS s3 Bucket.",
                "type": "text"
                }
            ]}
        ]},
        "reporter": {
            "id": "datamanagement@njoy.com"
        },
        "priority": {
            "id": "2"
        },
        "labels": [
            "fms_file_not_available"
        ]}
    })
    
    headers = {
        'Authorization': 'Basic ' + 'ATATT3xFfGF0BbE6eAMZ3jMcUO3LpGemJhfBytHy-PoGlTxcC8moAVCBmsX_z6pmnHXMANYnDSNw8QKYNii-1tcaPBdAx6rn6M72eSlvoLsGtLxVXlK_64K8CUQNHU7cNh-PQzaQGtu88VIiUP7f43LvjNflFmVq2QcIMgYZ4ecQ0__ulSzAsWU=AF772088',
        'Content-Type': 'application/json'
    }
    
    print("================= JIRA Request Built.              =================")
    response = requests.request("POST", url, headers=headers, data=payload)
    # jira_data_obj = json.loads(response.text)
    print(response.text)
    print("================= JIRA Ticket Raised Successfully. =================")
    return response
    
        
raise_jira_issue()