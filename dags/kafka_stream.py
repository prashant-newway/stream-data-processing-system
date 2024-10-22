from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'prashant-newway',
    'start_date': datetime(2024,10,21,14,00)  #datetime.today().strftime('%Y-%m-%d-%H:%M:%S')

}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    # errro handling , logging , any other type of data type , format 
    #print(res.json())
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):

    data = {}
    location = res['location']
    #data['id'] = uuid.uuid4()

    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data_from_api():
    import json
    res = get_data()
    res = format_data(res)
    print(res)
    print(json.dumps(res , indent=3))

with DAG('stream_data_processing',
         default_args=default_args,
         schedule ='@daily',
         catchup=False) as dag:
    
    streaming_task  = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data_from_api

    )


stream_data_from_api()