import psycopg2
import os
import pandas as pd
from errors import SqlDatabaseException
from dotenv import load_dotenv, dotenv_values
load_dotenv()

user_name = os.getenv('PG_USER_NAME')
user_password = os.getenv('PG_PASSWORD')

class SqlOrefDatabase:
    def __init__(self) -> None:
        # Initialize db connections 
        self.db_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'OrefAlerts',
            'user': user_name,
            'password': user_password,
        }


    def create_oref_alert_table(self, table_name:str):
        try:
            with psycopg2.connect(**self.db_params) as conn:
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


    def preparing_oref_data(self, data_list:list):
        '''data_list look like this: 
                    [
                    {'data': 'Gavim, Sapir College', 'date': '20.10.2023', 'time': '23:02:00', 'alertDate': '2023-10-20T23:02:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23055},
                    {'data': 'Sderot, Ivim, Nir Am', 'date': '20.10.2023', 'time': '23:01:59', 'alertDate': '2023-10-20T23:02:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23056},
                    {'data': 'Ashkelon - North', 'date': '20.10.2023', 'time': '22:00:23', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23053},
                    {'data': 'Zikim', 'date': '20.10.2023', 'time': '22:00:22', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23054},
                    {'data': 'Ashkelon - South', 'date': '20.10.2023', 'time': '22:00:13', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23051},
                    {'data': 'Ashkelon Southern Industrial Zone', 'date': '20.10.2023', 'time': '22:00:12', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23052}
                    ]'''
        data_tuples = [(d['data'], d['date'], d['time'], d['category_desc']) for d in data_list]
        return data_tuples


    def insert_alerts_to_oref_table(self, table_name:str, data_list:list):
        try:
            tuples_list = self.preparing_oref_data(data_list)
            print("--------------------------tuples list-----------------",tuples_list)
            with psycopg2.connect(**self.db_params) as conn:
                cursor = conn.cursor()
                QUERY = f"INSERT INTO {table_name} (settlement, date, time, alert_type) VALUES (%s, %s, %s, %s);"
                cursor.executemany(QUERY, tuples_list)
                conn.commit()

        except Exception as ex:
            raise SqlDatabaseException(ex)
        

    def retrieve_data_from_oref_table(self, table_name:str):
        try:
            with psycopg2.connect(**self.db_params) as conn:
                QUERY = f"SELECT * FROM {table_name};"
                df = pd.read_sql(QUERY, conn)
                return df

        except Exception as ex:
            raise SqlDatabaseException(ex)
        
    # delete all values in table: table_name
    def delete_oref_table(self, table_name:str):
        try:
            with psycopg2.connect(**self.db_params) as conn:
                cursor = conn.cursor()
                QUERY = f"DELETE FROM {table_name};"
                cursor.execute(QUERY)
                conn.commit()
        
        except Exception as ex:
            raise SqlDatabaseException(ex)
        

def main():
    oref_db = SqlOrefDatabase()
    # list_of_data = [
    #             {'data': 'Gavim, Sapir College', 'date': '20.10.2023', 'time': '23:02:00', 'alertDate': '2023-10-20T23:02:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23055},
    #             {'data': 'Sderot, Ivim, Nir Am', 'date': '20.10.2023', 'time': '23:01:59', 'alertDate': '2023-10-20T23:02:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23056},
    #             {'data': 'Ashkelon - North', 'date': '20.10.2023', 'time': '22:00:23', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23053},
    #             {'data': 'Zikim', 'date': '20.10.2023', 'time': '22:00:22', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23054},
    #             {'data': 'Ashkelon - South', 'date': '20.10.2023', 'time': '22:00:13', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23051},
    #             {'data': 'Ashkelon Southern Industrial Zone', 'date': '20.10.2023', 'time': '22:00:12', 'alertDate': '2023-10-20T22:00:00', 'category': 1, 'category_desc': 'Missiles', 'matrix_id': 1, 'rid': 23052}
    #             ]
    # oref_db.create_oref_alert_table("OrefTest")
    # oref_db.insert_alerts_to_oref_table("OrefTest", list_of_data)
    # oref_db.delete_oref_table("OrefTest")
if __name__ == "__main__":
    main()