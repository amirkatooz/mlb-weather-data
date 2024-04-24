import os
import requests
import re
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine


GAMES_URL = "https://statsapi.mlb.com/api/v1/schedule/games/?sportId=1&startDate={start_date}&endDate={end_date}"
TEAM_ADDRESS_URL = "https://gist.githubusercontent.com/the55/2155142/raw/30a251395cd3c04771f29f2a6295fc8849b73d11/mlb_stadium.json"
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast/?forecast_days=9&latitude={lat}&longitude={lng}&hourly=temperature_2m,rain,showers,snowfall,wind_speed_10m"

def make_api_call(url):
    """
    Takes a URL.
    Returns the JSON data from the input URL.
    Handles errors.
    
    """
    
    try:
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
        else:
            print('Request failed with status code:', r.status_code)
            print('Response:', r)
            return None
    except Exception as e:
        print('Request failed with error message:', str(e))
        return None
    
    return data


def camel_to_snake(col_name):
    """
    Takes a column name. Turns CamelCase to snake_case.
    
    """
    
    col_name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', col_name).lower()


def upload_to_s3(local_path, s3_bucket, s3_path):
    """
    Takes a local file and uploads it to a user specified s3 path.
    
    """
    
    load_dotenv()
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    s3 = session.resource('s3')

    BUCKET = s3_bucket
    s3.Bucket(BUCKET).upload_file(local_path, s3_path)


def write_to_postgres(df, table_name):
    """
    
    
    """
    
    engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
    df.to_sql(table_name, engine)
