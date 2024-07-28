import psycopg2
import os
import pandas as pd
from errors import SqlDatabaseException
from dotenv import load_dotenv, dotenv_values
load_dotenv()

user_name = os.getenv('PG_USER_NAME')
user_password = os.getenv('PG_PASSWORD')

class OrefAlertsDB:
    def __init__(self, table_name:str) -> None:
        self.db_url = os.getenv("DATABASE_URL")

        try:
            with psycopg2.connect(self.db_url) as conn:
                cursor = conn.cursor()
                QUERY = f"""CREATE TABLE IF NOT EXISTS {table_name} 
                            (settlement VARCHAR(100) NOT NULL, 
                            date VARCHAR(100) NOT NULL, 
                            time VARCHAR(100) NOT NULL, 
                            alert_type VARCHAR(100) NOT NULL);"""
                cursor.execute(QUERY)
                conn.commit()
        except Exception as ex:
            raise SqlDatabaseException(ex)


    def alets_json_to_tuple(self, alerts:list) -> list:
        ''' alerts look like this: 
                    [
                    {'data': 'Gavim, Sapir College', 'date': '20.10.2023', 'time': '23:02:00', 'alertDate': '2023-10-20T23:02:00', 'category': 1, 'category_desc': 'Hostile aircraft intrusion', 'matrix_id': 1, 'rid': 23055},
                    {'data': 'Sderot, Ivim, Nir Am', 'date': '20.10.2023', 'time': '23:01:59', 'alertDate': '2023-10-20T23:02:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23056},
                    {'data': 'Ashkelon - North', 'date': '20.10.2023', 'time': '22:00:23', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23053},
                    {'data': 'Zikim', 'date': '20.10.2023', 'time': '22:00:22', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Hostile aircraft intrusion', 'matrix_id': 1, 'rid': 23054},
                    {'data': 'Ashkelon - South', 'date': '20.10.2023', 'time': '22:00:13', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23051},
                    {'data': 'Ashkelon Southern Industrial Zone', 'date': '20.10.2023', 'time': '22:00:12', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23052}
                    ]'''
        for item in alerts:
            item['data'] = item['data'].lower()
        alerts_tuples = [(d['data'], d['date'], d['time'], d['category_desc']) for d in alerts]
        return alerts_tuples


    def insert_alerts_to_db(self, table_name:str, alerts:list) -> None:
        try:
            tuples_list = self.alets_json_to_tuple(alerts)
            with psycopg2.connect(self.db_url) as conn:
                cursor = conn.cursor()
                QUERY = f"INSERT INTO {table_name} (settlement, date, time, alert_type) VALUES (%s, %s, %s, %s);"
                cursor.executemany(QUERY, tuples_list)
                conn.commit()

        except Exception as ex:
            raise SqlDatabaseException(ex)
        

    def retrieve_data_from_oref_table(self, table_name:str) -> pd.DataFrame:
        try:
            with psycopg2.connect(self.db_url) as conn:
                query = f"SELECT * FROM {table_name};"
                df = pd.read_sql(query, conn)
                return df
                
        except Exception as ex:
            raise SqlDatabaseException(ex)
        


    # delete all values in table: table_name
    def delete_alerts_table(self, table_name:str) -> None:
        try:
            with psycopg2.connect(self.db_url) as conn:
                cursor = conn.cursor()
                QUERY = f"DELETE FROM {table_name};"
                cursor.execute(QUERY)
                conn.commit()
        
        except Exception as ex:
            raise SqlDatabaseException(ex)
        