import json
import psycopg2
import requests
from psycopg2 import errors
import datetime

with open('D:\\Organisation1.txt', 'r') as file:
    org_name = file.read().replace("\n"," ")
    
x=["Training","Training1","Test2","SpringBoot-Sample"]
for p in x:
    project_name=p

    r=requests.get("https://dev.azure.com/{0}/{1}/_apis/git/repositories?api-version=6.0".format(org_name,project_name))
    repoid=r.json()["value"][0]['id']

    r1 = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/repositories/{2}/commits?api-version=6.0'.format(org_name,project_name,repoid))
    no_commits = r1.json()["count"]
    
    committer_name=r1.json()["value"][0]["committer"]["name"]
    
    r2=requests.get("https://dev.azure.com/{0}/{1}/_apis/build/builds?api-version=6.0".format(org_name,project_name))
    no_builds = r2.json()["count"]
    
    r3=requests.get("https://dev.azure.com/{0}/{1}/_apis/git/pullrequests?api-version=6.0".format(org_name,project_name))
    no_pullrequests = r3.json()["count"]
    
    i=0
    x=[]
    while(i<no_commits):
        L = r1.json()["value"][i]["committer"]["name"]
        x.append(L)
        i=i+1
    no_commiters=(len(set(x)))

    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/repositories/{2}/refs?api-version=4.1'.format(org_name,project_name,repoid))
    branch_count = r.json()["count"]

    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/git/repositories/{2}/pushes?api-version=6.0'.format(org_name,project_name,repoid))
    push_count = r.json()["count"]


    r = requests.get('https://dev.azure.com/{0}/{1}/_apis/pipelines?api-version=6.0-preview.1'.format(org_name,project_name))
    pipeline_count = r.json()["count"]


    i=0
    while(i<1):
        org_name = r2.json()["value"][0]["requestedFor"]["displayName"]
        i=i+1

    #calculating avg_build_time
    msum=0
    ssum=0
    sum=0
    format_string = "%Y-%m-%d %H:%M:%S.%f"
    for j in r2.json()["value"]:
        f_time= j["finishTime"].replace('T',' ')[:-3]
        finish_time = datetime.datetime.strptime(f_time, format_string)
        s_time= j["startTime"].replace('T',' ')[:-3]
        start_time = datetime.datetime.strptime(s_time, format_string)
        diff=finish_time-start_time
        msum= msum+(int)(str(diff).split(":")[1])
        ssum= ssum+(float)(str(diff).split(":")[2])
        
    sum=msum+(ssum/100)
    avg_build_time=sum/float(r2.json()["count"])
     
    data=r2.json()["value"]
    i=1
    while i>0:
        b=data[0]["sourceBranch"]
        i=i-1
    b1=b.split("/")
    branch_name=b1[2]

    fcount=0
    for value in r2.json()["value"]:
        if(value["result"]=="failed"):
            fcount=fcount+1

    build_failed=fcount
    build_succeeded = no_builds-build_failed
    summary_date= datetime.date.today()
    build_suc_per= (build_succeeded/no_builds)*100
    build_fail_per= (build_failed/no_builds)*100
    Reported_Date = datetime.datetime.now()
    
    try:
        connection = psycopg2.connect(database="postgres", user="postgres", password="malli", host="127.0.0.1", port="5432")
        cursor = connection.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS commits
            (
                org_name varchar(20),project_name varchar(20),commit_id varchar(50),committer_name varchar(30),commit_date TIMESTAMP
            ); ''')
        
        print("Commits Table created successfully")
        
        i=0
        while(i<no_commits):
            cursor.execute("INSERT INTO commits(org_name,project_name,commit_id,committer_name,commit_date) VALUES (%s,%s,%s,%s,%s)",(org_name,project_name,r1.json()["value"][i]["commitId"],r1.json()["value"][i]["committer"]["name"],
                            r1.json()["value"][i]["committer"]["date"] ))                                                                                             
            i=i+1
        
        cursor.execute('''
           CREATE TABLE IF NOT EXISTS builds
           (
              org_name varchar(20),project_name varchar(20),id integer,build_number real,status varchar(20),
              result varchar(20),queuetime TIMESTAMP,starttime TIMESTAMP,finishtime TIMESTAMP
           ); ''')
        print("Build Table created successfully")
        
        i=0
        while(i<no_builds):
            cursor.execute("INSERT INTO builds(org_name,project_name,id,build_number,status,result,queueTime,startTime,finishTime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(org_name,project_name,r2.json()["value"][i]["id"],r2.json()["value"][i]["buildNumber"],
                                    r2.json()["value"][i]["status"],r2.json()["value"][i]["result"],r2.json()["value"][i]["queueTime"],r2.json()["value"][i]["startTime"],r2.json()["value"][i]["finishTime"] ))                                                                                             
            i=i+1
            
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pullrequests
           (
              org_name varchar(20),project_name varchar(20),pullRequestId integer,status varchar(20),createdBy varchar(20),createdDate TIMESTAMP,source_branch varchar(20),target_branch varchar(20)
           ); ''')
        
        print("Pull requests Table created successfully")
         
        i=0
        while(i<no_pullrequests):
            s_branch=str(r3.json()["value"][i]["sourceRefName"]).split("/")[2]
            t_branch=str(r3.json()["value"][i]["targetRefName"]).split("/")[2]
            cursor.execute("INSERT INTO pullrequests(org_name,project_name,pullRequestId,status,createdBy,createdDate,source_branch,target_branch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(org_name,project_name,r3.json()["value"][i]["pullRequestId"],r3.json()["value"][i]["status"],
                                    r3.json()["value"][i]["createdBy"]["displayName"],r3.json()["value"][i]["creationDate"],s_branch,t_branch ))                                                                                             
            i=i+1
        
        print("yes")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS summary
            (
                Reported_Date TIMESTAMP,Org_name varchar(20),project_name varchar(20),branch_name varchar(10),
                branch_count integer,push_count integer,pipeline_count integer,
                summary_date DATE,no_commits integer,no_builds integer,avg_build_time float,
                build_succeeded integer,build_failed integer,build_suc_per float(4),no_commiters integer,pull_requests integer
            ); ''')
                                          
        cursor.execute("INSERT INTO summary(Reported_Date,Org_name,project_name,branch_name,branch_count,push_count,pipeline_count,summary_date,no_commits,no_builds,avg_build_time,build_succeeded,build_failed,build_suc_per,no_commiters,pull_requests) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(Reported_Date,org_name,
                            project_name,branch_name,branch_count,push_count,pipeline_count,summary_date,no_commits,no_builds,avg_build_time,build_succeeded,build_failed,build_suc_per,no_commiters,no_pullrequests))                                                                                                                       
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



    