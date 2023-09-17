import datetime
import json
import random
import requests

from database_utils import *
from time import sleep

# generate seed for reproducible 'random' results
random.seed(100)

# instantiate a new database connector object
new_connector = AWSDBConnector()

def post_record_to_stream(invoke_url: str, stream: str, record_dict: dict):
    '''Creates payload of correct format for posting to Kinesis stream, and uses
    requests library to send payload to invoke_url via PUT request
    '''
    # iterate over record dictionary and check if any values are of type datetime
    for key, value in record_dict.items():
        # if so, convert to string
        if type(value) == datetime.datetime:
            record_dict[key] = value.strftime("%Y-%m-%d %H:%M:%S")
    # create payload from dictionary in format that can be uploaded to stream
    payload = json.dumps({
        "StreamName": stream,
        "Data": record_dict,
        "PartitionKey": stream[23:]    
    })
    # create header string for PUT request
    headers = {'Content-Type': 'application/json'}
    # make request to API
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(payload)
    print(response.status_code)
    

def run_infinite_post_data_loop():
    '''Iterates infinitely, establishing database connection and calling method
    to obtain random records from three separate tables
    '''
    while True:
        # pause for a random length of time between 0 and 2 seconds
        sleep(random.randrange(0, 2))
        # generate a random row number between 0 and 11000
        random_row = random.randint(0, 11000)
        # create database connection
        engine = new_connector.create_db_connector()
        with engine.connect() as connection:
            # get row record for random row from three separate tables
            pin_result = get_record_from_table("pinterest_data", connection, random_row)
            geo_result = get_record_from_table("geolocation_data", connection, random_row)
            user_result = get_record_from_table("user_data", connection, random_row)
            # post result to Kafka cluster via API
            post_record_to_stream("https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-pin/record", "streaming-1215be80977f-pin", pin_result)
            post_record_to_stream("https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-geo/record", "streaming-1215be80977f-geo", geo_result)
            post_record_to_stream("https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-user/record", "streaming-1215be80977f-user", user_result)


if __name__ == "__main__":
    print('Working')
    run_infinite_post_data_loop()
    