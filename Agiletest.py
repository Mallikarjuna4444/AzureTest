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
    print(message)
    postToPostgrest = requests.post("http://52.172.45.45:3000/build",data=message,headers=HEADERS)
    postToPostgrestRC = postToPostgrest.status_code
    print(postToPostgrestRC)
    if(postToPostgrestRC==200 or postToPostgrestRC==201):
        print("pushed into postgres")
    else:
        print("Error status Code get tools details "+ str(postToPostgrestRC))

def builddata(toolDetails, inputJson, postgrestDetails):
    
    project_name = inputJson['ci']['project']['project_name']
    org_name = inputJson['ci']['project']['org_name']
    r=requests.get("https://dev.azure.com/{0}/{1}/_apis/build/builds?api-version=6.0".format(org_name,project_name))
    no_builds = r.json()["count"]
    
    i=0
    while(i<no_builds):
        id=r.json()["value"][i]["id"]
        build_number=r.json()["value"][i]["buildNumber"]
        status=r.json()["value"][i]["status"]
        result=r.json()["value"][i]["result"]
        format_string = "%Y-%m-%d %H:%M:%S.%f"
        queuetime= r.json()["value"][i]["queueTime"]
    
        starttime=r.json()["value"][i]["startTime"]
       
        finishtime=r.json()["value"][i]["finishTime"]
        
        
        print(finishtime)
        msg = {"org_name": org_name,"project_name": project_name,
                "id":id,"build_number":build_number,
                "status":status,"result":result,
                "queuetime":queuetime,
                "starttime":starttime,
                "finishtime":finishtime
            }
        msgBody = json.dumps(msg)
        pushToPostgrest(postgrestDetails, msgBody, "build")
        i=i+1
    
    
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
    
    builddata(toolDetails, inputJson, postgrestDetails)

main()