from sqlalchemy import text
from botocore import UNSIGNED 
from botocore.config import Config
import pandas as pd
import boto3 
import re
import requests
import tabula
import jpype
jpype.startJVM(jpype.getDefaultJVMPath())


class DataExtractor:
    '''
    CLASS OF DATA EXTRACTION METHODS
    Defines a class of methods for extracting data in different formats as dataframes from various AWS servers.
    '''
    
    def read_rds_table(self, datco_instance, table_name):
        '''
        Reads the table from the RDS database as a pandas DataFrame.
        Attributes:
            datco_instance: instance of sqlalchemy database engine, initiated using DatabaseConnector.init_db_engine()
            table_name (string): name of table as found in database
        '''
        engine = datco_instance.init_db_engine()
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name}"))
            df = pd.DataFrame(result.fetchall())
            df.columns = result.keys()
            print(f"READING TABLE '{table_name}': \n")
            return df

    def upload_to_db(self, datco_instance, df, up_table_name):
        '''
        Uploads cleaned data in to TABLE sales_data in local database.
        Attributes:
            datco_instance: instance of sqlalchemy database engine, initiated using DatabaseConnector.init_db_engine()
            table_name (string): User-defined name to save dataframe as table in local database
        '''
        engine = datco_instance.init_sql_database()
        with engine.connect():
            df.to_sql(up_table_name, engine, if_exists='replace')
        print(f'Uploaded dataframe as {up_table_name}.', '\n'*2, '----'*30, '\n'*3)

    def retrieve_pdf_data(self, pdf_path):
        '''
        Reads .pdf from link as DataFrame.
        Attributes:
            pdf_path (string): URL to .pdf file
        '''
        pdf_tables_list = tabula.read_pdf(pdf_path, pages='all')
        df_pdf = pd.concat(pdf_tables_list)
        df_pdf = df_pdf.reset_index(drop=True)
        print('\n')
        print(f"READING TABLE '{df_pdf.columns[0]}s': \n")
        return df_pdf

    def list_number_of_stores(self, endpoint_link, headers_dict):
        '''
        Returns the number of stores owned by company from AWS endpoint as integer.
        Attributes:
            endpoint_link (string): AWS API request URL
            headers_dict (dict): Dictionary with AWS API key
        '''
        response = requests.get(endpoint_link, headers=headers_dict)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

    def retrieve_stores_data(self, endpoint_link, headers_dict, list_length):
        '''
        Extracts the stores from AWS API as a DataFrame.
        Attributes:
            endpoint_link (string): AWS API request URL
            headers_dict (dict): Dictionary with AWS API key
            list_length (int): Number of stores to download found using DataExtractor.clean_store_data()
        '''
        print(f"READING TABLE 'stores_data': \n")
        data = []
        for store_number in range(list_length):
            response = requests.get(endpoint_link+str(store_number), headers=headers_dict)
            if response.status_code == 200:
                row = response.json()
                data.append(row)
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        df = pd.DataFrame(data)
        return df

    def extract_from_s3(self, s3_path):
        '''
        Reads CSV file from AWS S3 bucket via API as pandas DataFrame
        Attributes:
            s3_path (string): AWS S3 path to CSV file
        '''
        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED), region_name='eu-west-1')
        split_path = re.split(r'/', s3_path)
        s3_bucket_name = split_path[-2]
        object_key = split_path[-1]
        print(f"READING TABLE '{object_key}': \n")
        object = s3.Object(s3_bucket_name, object_key)
        s3_csv = object.get()['Body']
        dataframe = pd.read_csv(s3_csv)
        return dataframe

    def retrieve_json_data(self, json_link):
        '''
        Reads JSON file from link as pandas DataFrame
        Attributes:
            json_link (string): URL to JSON file
        '''
        split_path = re.split(r'/', json_link)[-1]
        print(f"READING TABLE '{split_path}': \n")
        response = requests.get(json_link)
        dict_response = response.json()
        dataframe = pd.DataFrame(dict_response)
        return dataframe

