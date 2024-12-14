from sqlalchemy import create_engine, inspect
import yaml
import psycopg2



class DatabaseConnector:
    '''
    CLASS FOR INITIATING AND ACCESSING DATABASES
    Defines a class of methods for accessing AWS server using a .yaml credentials file, 
    uploading clean dataframes as tables in a local database, and accessing the database to conduct SQL queries.
    '''

    def __init__(self, yaml_file):
        '''Initiates class.
        Attributes:
            yaml_file (.yaml): YAML file stored in project folder'''
        self.yaml_file = yaml_file

    def read_db_creds(self):
        '''Reads the credentials .yaml file and return a dictionary of the credentials'''
        with open(self.yaml_file, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
        return data_loaded
    
    def init_db_engine(self): 
        '''Reads credentials from read_db_creds, initialise and return an sqlalchemy database engine'''
        creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self):  
        '''Lists all the tables in the database'''
        # Use engine from init_db_engine
        engine = self.init_db_engine()
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        print('\n Table names:', table_names, '\n')

    def init_sql_database(self):
        '''Reads credentials from read_db_creds, open sales_data database'''
        creds = self.read_db_creds()
        PASSWORD = creds['SQL_PASSWORD']
        DATABASE = creds['SQL_DATABASE']
        engine = create_engine(f'postgresql://postgres:{PASSWORD}@localhost:5432/{DATABASE}')
        return engine
    
    def connect_sql_database(self, COMMAND, ALTER=False):
        '''Runs and closes SQL query from local database with credentials from read_db_creds.
        Attributes:
            COMMAND (string): SQL command as string
            ALTER (boolean): If command alters the table, so produces no printable output, set ALTER=True'''
        creds = self.read_db_creds()
        PASSWORD = creds['SQL_PASSWORD']
        DATABASE = creds['SQL_DATABASE']
        connection = psycopg2.connect(host="localhost", port='5432', database=DATABASE, user="postgres", password=PASSWORD)
        cursor = connection.cursor()
        cursor.execute(COMMAND)
        if ALTER == False:
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        # Commit changes
        connection.commit()
        # Close the cursor
        cursor.close()
        # Close the connection
        connection.close()
