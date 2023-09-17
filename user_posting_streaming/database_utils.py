import os
import sqlalchemy

from dotenv import load_dotenv
from sqlalchemy import text

# load environment variables for database credentials
load_dotenv()


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


def get_record_from_table(table: str, connection, row_number: int):
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
