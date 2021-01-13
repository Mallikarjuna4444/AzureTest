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

# get tool details from the riglet
def getToolDetails(rigUrl, rigletName, toolName):
    # def message='"rigletName":"'+rigletName+'","toolName":"'+toolName+'"'
   
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
    postToPostgrest = requests.post("http://52.172.45.45:3000/java",data=message,headers=HEADERS)
    postToPostgrestRC = postToPostgrest.status_code
    print(postToPostgrestRC)
    if(postToPostgrestRC==200 or postToPostgrestRC==201):
        print("pushed into postgres")
    else:
        print("Error status Code get tools details "+ str(postToPostgrestRC))

# Save Project details to Rig
def saveProjectDetails(rigletName, rigUrl, toolName):
    now = datetime.datetime.now()
    now1 = (json.dumps(now, default = myconverter)) 
    message = json.dumps({"rigletName": rigletName, "toolName": toolName, "lastRunTime": now1})
    saveProjectDetail = requests.post(rigUrl + "/api/riglets/collector/saveToolProjectInfo", data=message,
                                      headers=HEADERS)

    saveProjectDetailRC = saveProjectDetail.status_code
    print(saveProjectDetailRC)
    if (saveProjectDetailRC == 200 or saveProjectDetailRC == 201):
        print(saveProjectDetail.json())
    else:
        print("Error status Code of Save Project Details - " + str(saveProjectDetailRC))

def metricOne(toolDetails, inputJson, postgrestDetails):
    
    project_name = inputJson['ci']['project']['project_name']
    org_name = inputJson['ci']['project']['org_name']

    
     # calculating pull requests
    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/pullrequests?api-version=6.0'.format(org_name, project_name))
    pull_requests = r.json()["count"]

    # Getting repo info
    r = requests.get("https://dev.azure.com/{0}/{1}/_apis/git/repositories?api-version=6.0".format(org_name, project_name))
    repoid = r.json()["value"][0]['id']

    # calculating no of commits
    r = requests.get(
        'https://dev.azure.com/{0}/{1}/_apis/git/repositories/{2}/commits?api-version=6.0'.format(org_name, project_name,
                                                                                                  repoid))
    commit_count = r.json()["count"]
    
    msg = {"pull_count" : pull_count,"no_commits": commit_count}
 
    msgBody = json.dumps(msg)

    pushToPostgrest(postgrestDetails, msgBody, "java")

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
    #metricOne(toolDetails, inputJson, postgrestDetails)


    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/pullrequests?api-version=6.0'.format(org_name,project_name))
    pull_requests = r.json()["count"]


    r=requests.get("https://dev.azure.com/{0}/{1}/_apis/git/repositories?api-version=6.0".format(org_name,project_name))
    repoid=r.json()["value"][0]['id']

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

    i=0
    while(i<1):
        org_name = r1.json()["value"][0]["requestedFor"]["displayName"]
        i=i+1

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
    build_suc_per= (build_succeeded/no_builds)*100
    build_fail_per= (build_failed/no_builds)*100
    Reported_Date = datetime.datetime.now()

    
    # pushing data into postgres
    try:
        connection = psycopg2.connect(database="postgres", user=postgrestoolUsername, password=postgrestoolPassword, host=postgresUrl,
                                      port=port)
        cursor = connection.cursor()
        print(connection.get_dsn_parameters())
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS azure5
            (
                Reported_Date TIMESTAMP,Org_name varchar(20),project_name varchar(20),branch_name varchar(10),
                branch_count integer,push_count integer,pipeline_count integer,
                summary_date DATE,no_commits integer,no_builds integer,avg_build_time float,
                build_succeeded integer,build_failed integer,build_suc_per float(4),no_commiters integer,pull_requests integer
            ); ''')
        
        print("Table created successfully")

        cursor.execute("select * from azure5")
        size=(len(cursor.fetchall()))
        
        if(size==0):
            cursor.execute("INSERT INTO azure5(Reported_Date,Org_name,project_name,branch_name,branch_count,push_count,pipeline_count,summary_date,no_commits,no_builds,avg_build_time,build_succeeded,build_failed,build_suc_per,no_commiters,pull_requests) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(Reported_Date,org_name,
                            project_name,branch_name,branch_count,push_count,pipeline_count,summary_date,no_commits,no_builds,avg_build_time,build_succeeded,build_failed,build_suc_per,no_commiters,pull_requests))                                                                                                                                  
            print("Record Inserted Successfully")

        
        ss="select * from azure5 where summary_date=%s"
        cursor.execute(ss,(summary_date,))
        size1=(len(cursor.fetchall()))
        
        check=0
        while(size1>0):
            cursor.execute(ss,(summary_date,))
            if(cursor.fetchall()[size1-1][2]==project_name):
                check=1
                break
            size1=size1-1
    
        rd1 = str(datetime.datetime.now()).split(" ")[0]
        
        sql = """ UPDATE azure5
                    SET project_name = %s,branch_name = %s,branch_count=%s,push_count=%s,pipeline_count=%s,summary_date= %s,no_commits= %s,
                    no_builds= %s,avg_build_time= %s,build_succeeded= %s,build_failed= %s,build_suc_per= %s,
                    no_commiters= %s,pull_requests= %s
                    WHERE org_name = %s and project_name = %s and summary_date = %s"""

        cursor.execute("select * from azure5")
        size= len(cursor.fetchall())
        cursor.execute("select * from azure5")
        s_date=(cursor.fetchall()[size-1][7])
        
        
        if(size>0):
            if(s_date==summary_date and check):
                cursor.execute(sql, (project_name,branch_name,branch_count,push_count,pipeline_count,summary_date,no_commits,no_builds,avg_build_time,build_succeeded,build_failed,build_suc_per,3,pull_requests,org_name,project_name,summary_date))
                print("Table updated")
            else:
                cursor.execute("INSERT INTO azure5(Reported_Date,Org_name,project_name,branch_name,branch_count,push_count,pipeline_count,summary_date,no_commits,no_builds,avg_build_time,build_succeeded,build_failed,build_suc_per,no_commiters,pull_requests) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(Reported_Date,org_name,
                            project_name,branch_name,branch_count,push_count,pipeline_count,summary_date,no_commits,no_builds,avg_build_time,build_succeeded,build_failed,build_suc_per,no_commiters,pull_requests))                                                                                                                                  
                print("Record inserted successfully")
                    
                
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


main()
