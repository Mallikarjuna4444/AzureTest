import json
import psycopg2
import datetime
import requests
from psycopg2 import errors


with open('D:\\Organisation1.txt', 'r') as file:
    Org_name = file.read().replace("\n"," ")

r3 = requests.get('https://dev.azure.com/{0}/Test2/_apis/git/repositories/9ea00b21-2fee-405d-81de-d810a1dd243e/commits?api-version=6.0'.format(Org_name))
d3 = r3.json()
count = d3["count"]

r = requests.get('https://dev.azure.com/{0}/Test2/_apis/git/repositories/9ea00b21-2fee-405d-81de-d810a1dd243e/pullrequests?api-version=6.0'.format(Org_name))
d = r.json()
pull_count = d["count"]

r1 = requests.get("https://dev.azure.com/{0}/Test2/_apis/build/builds?api-version=6.0".format(Org_name))
d1=r1.json()
no_builds = d1["count"]

i=0
while(i<1):
    org_name = r1.json()["value"][0]["requestedFor"]["displayName"]
    print(org_name)
    i=i+1
    
sum=0
for j in d1["value"]:
    sum=sum+((int)(j["finishTime"].split(":")[1])-(int)(j["startTime"].split(":")[1]))
avg_build_time=sum/float(r1.json()["count"])

data=r1.json()["value"]
i=1
while i>0:
    b=data[0]["sourceBranch"]
    i=i-1
b1=b.split("/")
branch=b1[2]
project_name=data[0]["project"]["name"]

fcount=0
r2=requests.get("https://dev.azure.com/{0}/Test2/_apis/build/builds?api-version=6.0".format(Org_name))
d2=r2.json()["value"]
for value in d2:
    if(value["result"]=="failed"):
        fcount=fcount+1

no_failed=fcount
no_succeeded = no_builds-no_failed
summary_date = "2020-10-24T00:00:00Z"
build_suc_per= (no_succeeded/no_builds)*100
build_fail_per= (no_failed/no_builds)*100
Reported_Date = datetime.datetime.now()

try:
    connection = psycopg2.connect(database="postgres", user="postgres", password="malli", host="127.0.0.1", port="5432")
    cursor = connection.cursor()
    print(connection.get_dsn_parameters())
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS azure1
           (
              Reported_Date TIMESTAMP,Org_name varchar(20),project_name varchar(20),branch varchar(10),
              summary_date TIMESTAMP,No_commits integer,no_builds integer,avg_build_time float,
              Build_succeeded integer,Build_failed integer,build_suc_per float(4),No_of_commiters integer,pull_requests integer
           ); ''')
    
    print("Table created successfully")

	
    cursor.execute("INSERT INTO azure1(Reported_Date,Org_name,project_name,branch,summary_date,No_commits,no_builds,avg_build_time,Build_succeeded,Build_failed,build_suc_per,No_of_commiters,pull_requests) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",( Reported_Date,org_name,
                        project_name,branch,summary_date,count,no_builds,avg_build_time,no_succeeded,no_failed,build_suc_per,1,pull_count));                                                                                                                              
        
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



