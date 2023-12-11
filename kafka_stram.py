from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging



default_args={
    'owner': 'Mohamed AIT LAADIK',
    'start_date': datetime(2023,12,4,14,00)
}
def get_data():
    import json
    import requests
    
    res=requests.get("https://randomuser.me/api/")
    res=res.json()
    res=res['results'][0]
    
    return res

def format_data(res):
    data={}
    location=res['location']
    data['firstname']=res['name']['first']
    data['lastname']=res['name']['last']
    data['gender']=res['gender']
    data["address"]=f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['post_code']=location['postcode']
    data['email']=res['email']
    data['username']=res['login']['username']
    data['dob']=res['dob']['date']
    data['registered_date']=res['registered']['date']
    data['phone']=res['phone']
    data['picture']=res['picture']['medium']
    
    return data

def stream_data():
    from kafka import KafkaProducer
    import json
    import time
    
    # initiate current time
    current_time=time.time()
    # initiate broker
    producer= KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    
    # send data every minute to broker
    
    while True:
        if time.time() > current_time + 60:
            break
        try:
            res=get_data()  
            res=format_data(res)
            producer.send('users_created',json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue
with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    streaming_task=PythonOperator(task_id='stream_data_from_api',
                                  python_callable=stream_data)

stream_data()