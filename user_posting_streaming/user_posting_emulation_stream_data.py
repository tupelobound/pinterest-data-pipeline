import random

from database_utils import *
from time import sleep

# generate seed for reproducible 'random' results
random.seed(100)

# instantiate a new database connector object
new_connector = AWSDBConnector()


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
            post_record_to_API("PUT", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-pin/record", pin_result, "streaming-1215be80977f-pin")
            post_record_to_API("PUT", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-geo/record", geo_result, "streaming-1215be80977f-geo")
            post_record_to_API("PUT", "https://hltnel789h.execute-api.us-east-1.amazonaws.com/Production/streams/streaming-1215be80977f-user/record", user_result, "streaming-1215be80977f-user")


if __name__ == "__main__":
    print('Working')
    run_infinite_post_data_loop()
    