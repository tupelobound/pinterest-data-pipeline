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
    using SQLAlchemy and acquiring records from the connected database
    '''
    def __init__(self):
        self.HOST = os.getenv('RDSHOST')
        self.USER = os.getenv('RDSUSER')
        self.PASSWORD = os.getenv('RDSPASSWORD')
        self.DATABASE = os.getenv('RDSDATABASE')
        self.PORT = os.getenv('RDSPORT')
        self.pin_result = {}
        self.geo_result = {}
        self.user_result = {}
    
    def create_db_connector(self):
        '''Uses sqlalchemy.create_engine() method to generate connection engine
        using credentials contained in class attributes. Returns engine object.
        '''
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:"
            f"{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine
    
    def get_record_from_table(self, table: str, connection, row_number: int):
        '''
        Generates a query string from table name and row number arguments and
        executes that query string on a given database connection to obtain a
        row record from a database
        '''
        query_string = text(f"SELECT * FROM {table} LIMIT {row_number}, 1")
        selected_row = connection.execute(query_string)
        for row in selected_row:
            result = dict(row._mapping)
        return result
    
    def connect_and_get_records(self):
        '''
        Generates a random integer for selecting a random row from the database,
        creates a database connection, and then obtains said row record from each
        of the three tables in the database
        '''
        # generate a random row number between 0 and 11000
        random_row = random.randint(0, 11000)
        # create database connection
        engine = self.create_db_connector()
        with engine.connect() as connection:
            # get row record for random row from three separate tables
            self.pin_result = self.get_record_from_table("pinterest_data", connection, random_row)
            self.geo_result = self.get_record_from_table("geolocation_data", connection, random_row)
            self.user_result = self.get_record_from_table("user_data", connection, random_row)


def post_record_to_API(*args):
    '''Creates payload of correct format for posting to API, and uses
    requests library to send payload to invoke_url via PUT or POST request
    '''
    # iterate over record dictionary and check if any values are of type datetime
    for key, value in args[2].items():
        # if so, convert to string
        if type(value) == datetime.datetime:
            args[2][key] = value.strftime("%Y-%m-%d %H:%M:%S")
    # create payload from dictionary in format that can be uploaded to API
    # if there are 4 arguments, payload is going to Kinesis stream API
    if len(args) == 4:
        # create header string for request
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({
            "StreamName": args[3],
            "Data": args[2],
            "PartitionKey": args[3][23:]    
        })
    # if there are 3 arguments, payload is going to Kafka batch API
    elif len(args) == 3:
        # create header string for POST request
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        payload = json.dumps({
            "records": [
                {
                    "value": args[2]
                }
            ]     
        })
    
    # make request to API
    response = requests.request(args[0], args[1], headers=headers, data=payload)
    print(payload)
    print(response.status_code)


def run_infinitely(func):
    '''Iterates infinitely, establishing database connection and calling method
    to obtain random records from three separate tables
    '''
    def inner():
        while True:
            # pause for a random length of time between 0 and 2 seconds
            sleep(random.randrange(0, 2))
            func()
    
    return inner
