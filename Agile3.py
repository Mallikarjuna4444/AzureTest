import json
import psycopg2
import sys
import requests
import datetime
from psycopg2 import errors
import json, requests, base64
from requests.exceptions import HTTPError

HEADERS = {'Content-Type': 'application/json', 'Accept': '*/*'}

def myconverter(o):
 if isinstance(o, datetime.datetime):
    return o.__str__()
def myconverter1(o):
 if isinstance(o, datetime.date):
    return o.__str__()
# get tool details from the riglet

def getToolDetails(rigUrl, rigletName, toolName):
   
    message = json.dumps({"rigletName": rigletName, "toolName": toolName})
    toolDetails = requests.post(rigUrl + "/api/riglets/connectorServerDetails", data=message, headers=HEADERS)
    toolDetailsRC = toolDetails.status_code
    print(toolDetailsRC)

    if (toolDetailsRC == 200 or toolDetailsRC == 201):
        
        toolJsonValue = toolDetails.json()
        user = toolJsonValue['username']
        password = toolJsonValue['password']
        toolUrl = toolJsonValue['url']
        toolsJson = {"user": user, "password": password, "toolUrl": toolUrl}
        return toolsJson
    else:
        print("Error status Code get tools details " + str(toolDetailsRC))
        print(toolDetails.json())
        return


#push to Postgrest
def pushToPostgrest(postgrestInfo, msgBody, dbName):

    message = msgBody
    #print(message)
    postToPostgrest = requests.post("http://52.172.45.45:3000/azure",data=message,headers=HEADERS)
    postToPostgrestRC = postToPostgrest.status_code
    print(postToPostgrestRC)
    if(postToPostgrestRC==200 or postToPostgrestRC==201):
        print("pushed into postgres")
    else:
        print("Error status Code get tools details "+ str(postToPostgrestRC)+str(postToPostgrest))

def metricOne(toolDetails, inputJson, postgrestDetails):
    
    project_name = inputJson['ci']['project']['project_name']
    org_name = inputJson['ci']['project']['org_name']

    #calculating pull_requests
    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/pullrequests?api-version=6.0'.format(org_name,project_name))
    pull_requests = r.json()["count"]

    #Getting repoid
    r=requests.get("https://dev.azure.com/{0}/{1}/_apis/git/repositories?api-version=6.0".format(org_name,project_name))
    repoid=r.json()["value"][0]['id']
    
    #calculating no_commits
    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/repositories/{2}/commits?api-version=6.0'.format(org_name,project_name,repoid))
    no_commits = r.json()["count"]

    i=0
    x=[]
    while(i<no_commits):
        L = r.json()["value"][i]["committer"]["name"]
        x.append(L)
        i=i+1
    no_commiters=(len(set(x)))

    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/repositories/{2}/refs?api-version=4.1'.format(org_name,project_name,repoid))
    branch_count = r.json()["count"]

    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/repositories/{2}/pushes?api-version=6.0'.format(org_name,project_name,repoid))
    push_count = r.json()["count"]


    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/pipelines?api-version=6.0-preview.1'.format(org_name,project_name))
    pipeline_count = r.json()["count"]


    r1 = requests.get("https://dev.azure.com/{0}/{1}/_apis/build/builds?api-version=6.0".format(org_name,project_name))
    no_builds = r1.json()["count"]

    #calculating avg_build_time
    msum=0
    ssum=0
    sum=0
    format_string = "%Y-%m-%d %H:%M:%S.%f"
    for j in r1.json()["value"]:
        f_time= j["finishTime"].replace('T',' ')[:-3]
        finish_time = datetime.datetime.strptime(f_time, format_string)
        s_time= j["startTime"].replace('T',' ')[:-3]
        start_time = datetime.datetime.strptime(s_time, format_string)
        diff=finish_time-start_time
        msum= msum+(int)(str(diff).split(":")[1])
        ssum= ssum+(float)(str(diff).split(":")[2])
        
    sum=msum+(ssum/100)
    avg_build_time=sum/float(r1.json()["count"])
    data=r1.json()["value"]
    i=1
    while i>0:
        b=data[0]["sourceBranch"]
        i=i-1
    b1=b.split("/")
    branch_name=b1[2]
    project_name=data[0]["project"]["name"]

    fcount=0
    for value in r1.json()["value"]:
        if(value["result"]=="failed"):
            fcount=fcount+1

    build_failed=fcount
    build_succeeded = no_builds-build_failed
    summary_date= datetime.date.today()
    print(summary_date)
    build_suc_per= (build_succeeded/no_builds)*100
    build_fail_per= (build_failed/no_builds)*100
    Reported_Date = datetime.datetime.now()
    print(type(Reported_Date))
    
    msg = {"reported_date":myconverter(Reported_Date),"org_name": org_name,"project_name":project_name,
            "branch_name":branch_name,"branch_count":branch_count,"push_count":push_count,
           "pipeline_count":pipeline_count,"summary_date":myconverter1(summary_date),"no_commits":no_commits,
           "no_builds":no_builds,"avg_build_time":avg_build_time,
           "build_succeeded":build_succeeded,"build_failed":build_failed,
           "build_suc_per":build_suc_per,"no_commiters":no_commiters,"pull_requests":pull_requests
           }
 
    msgBody = json.dumps(msg)
    #print(msgBody)
    pushToPostgrest(postgrestDetails, msgBody, "azure")
    
    

# main function to trigger the collector sequence
def main():
    rigUrl = sys.argv[1]
    inputJson = json.loads(sys.argv[2])
    
    # depends on the input json schema for which the connector is written
    project_name = inputJson['ci']['project']['project_name']
    org_name = inputJson['ci']['project']['org_name']
    rigletName = inputJson['riglet_info']['name']
    toolName=inputJson['ci']['name']

    # extract params based on the requirement and inputJson structure
    
    #AzureDevOps Details
    toolDetails = getToolDetails(rigUrl, rigletName, toolName)
    postgrestoolDetails = getToolDetails(rigUrl, rigletName, "Postgres")
    print(postgrestoolDetails)
    toolUrl = toolDetails['toolUrl']
    toolUsername = toolDetails['user']
    toolPassword = toolDetails['password']

    #Postgres Details
    postgrestoolUrl = postgrestoolDetails['toolUrl']
    postgresUrl = (str(postgrestoolUrl).split(":")[1][2:])
    port = (str(postgrestoolUrl).split(":")[2])
    postgrestoolUsername = postgrestoolDetails['user']
    postgrestoolPassword = postgrestoolDetails['password']
    
    
    postgrestDetails = {"toolUrl": "http://52.172.45.45:3000"}
    metricOne(toolDetails, inputJson, postgrestDetails)

main()