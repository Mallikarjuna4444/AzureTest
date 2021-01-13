import json
import psycopg2
import requests
from psycopg2 import errors
import datetime

with open('D:\\Organisation1.txt', 'r') as file:
    org_name = file.read().replace("\n"," ")
    
r=requests.get("https://dev.azure.com/{0}/Training/_apis/build/builds?api-version=6.0".format(org_name))
no_builds=r.json()["count"]
print(no_builds)

try:
    connection = psycopg2.connect(database="postgres", user="postgres", password="malli", host="127.0.0.1", port="5432")
    cursor = connection.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS builds
           (
              id integer,build_number float(2),status varchar(20),result varchar(20),queue_time TIMESTAMP,start_time TIMESTAMP,finish_time TIMESTAMP
           ); ''')
    
    print("Table created successfully")
    i=0
    while(i<no_builds):
        cursor.execute("INSERT INTO builds(id,build_number,status,result,queue_time,start_time,finish_time) VALUES (%s,%s,%s,%s,%s,%s,%s)",( r.json()["value"][i]["id"],r.json()["value"][i]["buildNumber"],
                            r.json()["value"][i]["status"],r.json()["value"][i]["result"],r.json()["value"][i]["queueTime"],r.json()["value"][i]["startTime"],r.json()["value"][i]["finishTime"]))                                                                                             
        i=i+1
        
                                                                                                                                 
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



