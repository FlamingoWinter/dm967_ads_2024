import os

from dotenv import load_dotenv

from fynesse.access.pipelines import resume_pipeline
from fynesse.common.db.db_setup import create_connection

load_dotenv(dotenv_path='../.env')

url = os.getenv('DB_URL')
user = os.getenv('DB_LOGIN')
password = os.getenv("DB_PASSWORD")
port = 3306

connection = create_connection(user, password=password, host=url, port=port,
                               database="ads_2024_part_2"
                               )

resume_pipeline(connection, "upload_work_type_relationships")
