from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

# Defining default arguments
# default_arguements = {
#     'owner':'muskan',
#     'start_date':datetime(2023,11,7)    
# }

# Python function to fetch data using API
def get_data():
    res = requests.get("https://randomuser.me/api/")
    data_res=res.json()['results'][0]
    return data_res

def format_data(data_res):
    data={}
    location = data_res['location']
    data['first_name'] = data_res['name']['first']
    data['last_name'] = data_res['name']['last']
    data['gender'] = data_res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} {location['city']} {location['state']} {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = data_res['email']
    data['username'] = data_res['login']['username']
    data['dob'] = data_res['dob']['date']
    data['registered_date'] = data_res['registered']['date']
    data['phone'] = data_res['phone']
    data['picture'] = data_res['picture']['medium']
    
    return data
    
def stream_data():
    res = get_data()
    formatted_data_res = format_data(res)
    print(json.dumps(formatted_data_res, indent=3))

# Creating DAG instance with default_arguements
# with DAG('data__stream_dag',
#          default_args=default_arguements,
#          schedule_interval='5 * * * *',
#          catchup=False) as dag:
    
#     # Creating task  that  calls python function
#     stream_task = PythonOperator(
#         task_id = 'data_stream_from_api_task',
#         python_callable=stream_data 
#     )
    
stream_data()