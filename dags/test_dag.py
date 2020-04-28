# AUTHOR: Sharath; 
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests as r
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import sqlite3
conn = sqlite3.connect('/Users/ksharath/projects/airflow_home/airflow.db')
#engine = create_engine('mysql+pymysql://root:@localhost/sharath')
#We can change the connection parameters here to change the database 


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# t1 is just a linux command to test the server
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)



oauth_data = [{
            "name": "animals",
            "API_Key":"Rb0222ZPLfQA9zqmk1DDXuYh5pKIgysPC9kIwjooIp8mBINIe7",
            "Secret":"9u8YIFulAavSx2VW4x5lzDwklW8DKlbo9nQsEhwx"
        }]

class ETL: #Class to instatiate the ETL with parameters 
    def __init__(self,category,ds,atype):
        self.base_url='https://api.publicapis.org/entries?'
        self.category=category
        self.https='false'
        self.cores='no'
        self.ds=ds
        if atype=='oauth':
            df=pd.DataFrame.from_records(oauth_data)
            self.API_Key=df[df['name']==category]['API_Key'][0]
            self.Secret=df[df['name']==category]['Secret'][0]
            self.atype=1
        else:
            self.atype=0
        
    def read_api(self): #function to access the normal get Request
        try:
            t=r.get(self.base_url+'category='+self.category+'&cors='+self.cores)
        except Exception as e:
            print("Failed to hit the API:%s"%str(e))
        if t.status_code ==200:
            print("Response Validated: Decoding json")
            j=t.json()
            if len(j['entries'])==j['count']:
                print("Header is validated")
                try:
                    df=pd.DataFrame.from_records(j['entries'])
                    print("Succesfully loaded df with rows")
                    self.df=df
                except Exception as e:
                    print("Error while Reading the json: %s"%str(e))
                    return None

    def read_oauth(self): #Function to hit oauth API
        try:

            data = {
            'grant_type': 'client_credentials',
            'client_id': self.API_Key,
            'client_secret': self.Secret 
            }
            h_response = r.post('https://api.petfinder.com/v2/oauth2/token', data=data)

            headers = { 'Authorization': 'Bearer %s'%h_response.json()['access_token']}
            d_response = r.get('https://api.petfinder.com/v2/%s/'%self.category, headers=headers)
            t=d_response.json()
        except Exception as e:
            print("Exception: %s"%str(e))

        try:
            df=pd.io.json.json_normalize(t['animals'])
            df.columns= [i.replace('.','_') for i in df.columns]
            df['tags']=df['tags'].astype(str)
            df['photos']=df['photos'].astype(str)
            df['videos']=df['videos'].astype(str)
            df['id']=df['id'].astype(int)
            print("Succesfully loaded df with rows from oauth")
            self.df=df
        except Exception as e:
            print("Error while Reading the json: %s"%str(e))
            return None

    def transform_data(self):
        self.df['load_date']=self.ds
        if self.atype==1:
            self.df.to_sql(self.category.replace('-','_').lower(), con = conn, if_exists='replace')   
        else:
            self.df.to_sql(self.category.replace('-','_').lower()+'_normal', con = conn, if_exists='replace')             
        print("Loaded data to stage table")





def start_service(ds, **kwargs):
    print(kwargs)
    print(ds)
    
    #d=ETL('animals','false','no',ds,'oauth')
    d=ETL('animals',ds,'oauth') #You can change the 3rd parameter to normal or OAUTH two access 2 types of APIs
    if d.atype==1:
        d.read_oauth()
    else:
        d.read_api()
    d.transform_data()
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='ETL',
    provide_context=True,
    python_callable=start_service, # I will start the ETL service here
    op_kwargs={'key1': 'value1', 'key2': 'value2'}, #Not required for now
    dag=dag,
)

t1 >> run_this
