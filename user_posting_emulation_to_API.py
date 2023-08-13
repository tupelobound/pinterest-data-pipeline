import datetime
import json
import os
import random
import requests
import sqlalchemy

from dotenv import load_dotenv
from sqlalchemy import text
from time import sleep

# load environment variables for database credentials
load_dotenv()
# generate seed for reproducible 'random' results
random.seed(100)


class AWSDBConnector:
    '''This class contains methods for establishing a connection to a database 
    using SQLAlchemy
    '''
    def __init__(self):
        self.HOST = os.getenv('RDSHOST')
        self.USER = os.getenv('RDSUSER')
        self.PASSWORD = os.getenv('RDSPASSWORD')
        self.DATABASE = os.getenv('RDSDATABASE')
        self.PORT = os.getenv('RDSPORT')
    
    def create_db_connector(self):
        '''Uses sqlalchemy.create_engine() method to generate connection engine
        using credentials contained in class attributes. Returns engine object.
        '''
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:"
            f"{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine


# instantiate a new database connector object
new_connector = AWSDBConnector()


def get_record_from_table(table: str, connection, row_number: int):
    '''Generates a query string from table name and row number arguments and
    executes that query string on a given database connection to obtain a
    row record from a database
    '''
    query_string = text(f"SELECT * FROM {table} LIMIT {row_number}, 1")
    selected_row = connection.execute(query_string)
    for row in selected_row:
        result = dict(row._mapping)
    return result


def post_record_to_API(invoke_url: str, record_dict: dict):
    '''Creates payload of correct format for posting to API, and uses requests
    library to send payload to invoke_url via POST request
    '''
    # iterate over record dictionary and check if any values are of type datetime
    for key, value in record_dict.items():
        # if so, convert to string
        if type(value) == datetime.datetime:
            record_dict[key] = value.strftime("%Y-%m-%d %H:%M:%S")
    # create payload from dictionary in format that can be uploaded as message to API
    payload = json.dumps({
        "records": [
            {
                "value": record_dict
            }
        ]     
    })
    # create header string for POST request
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    # make request to API
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    print(response.status_code)


def run_infinite_post_data_loop():
    '''Iterates infinitely, establishing database connection and calling method
    to obtain random records from three separate tables'''
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
            post_record_to_API("https://do64ku0ks1.execute-api.us-east-1.amazonaws.com/prod/topics/1215be80977f.pin", pin_result)
            post_record_to_API("https://do64ku0ks1.execute-api.us-east-1.amazonaws.com/prod/topics/1215be80977f.geo", geo_result)
            post_record_to_API("https://do64ku0ks1.execute-api.us-east-1.amazonaws.com/prod/topics/1215be80977f.user", user_result)


if __name__ == "__main__":
    print('Working')
    run_infinite_post_data_loop()
    