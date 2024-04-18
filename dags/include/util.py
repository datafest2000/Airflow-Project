from dotenv import load_dotenv
import os
from sqlalchemy import create_engine



def get_api_key():
    '''
    This is a function to get the api credentialsb from the environment variable @.env

    Parameters: NULL
    return value: a tuple of api_id and api_key
    type: tuple
    '''
    load_dotenv()
    api_id = os.getenv('API_ID')
    api_key = os.getenv('API_KEY')
    

    return (api_id, api_key)

def get_database_conn():
    load_dotenv()
    db_user_name = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    port = os.getenv('DB_PORT')
    host = os.getenv('DB_HOST')

    return create_engine(f'postgresql://{db_user_name}:{db_password}@{host}:{port}/{db_name}')


   