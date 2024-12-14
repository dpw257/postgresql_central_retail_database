from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


'''
EXTRACT, CLEAN, UPLOAD DATA
Imports connection, data extraction and data cleaning functions to clean and upload data as tables in a local database.
The functions state the number of rows deleted, errors found and final length of table. 
'''

if __name__ == "__main__":
    # Read the credentials yaml file, initialise and return an sqlalchemy database engine
    datco = DatabaseConnector('db_creds.yaml')
    # List name of tables in database
    datco.list_db_tables()

    ## USER DATA
    # Extract the table containing user data and return a DataFrame.
    datex = DataExtractor()
    df_legacy_users = datex.read_rds_table(datco, 'legacy_users')
    df_legacy_users = df_legacy_users.copy()
    # Clean user data
    datcl = DataCleaning()
    df_clean_user = datcl.clean_user_data(df_legacy_users)
    # Upload dataframe to database as table
    datex.upload_to_db(datco, df_clean_user, 'dim_users')

    ## CREDITCARD DATA
    # Read card_details from .pdf and clean
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_details = datex.retrieve_pdf_data(pdf_path)
    df_clean_card = datcl.clean_card_data(card_details)
    # Upload dataframe to database as table
    datex.upload_to_db(datco, df_clean_card, 'dim_card_details')

    ## STORES DATA
    # Read stores data via API - Number of stores
    stores_total_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    stores_total = datex.list_number_of_stores(stores_total_endpoint, headers)
    # Read stores data via API - Store details
    stores_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    stores_data = datex.retrieve_stores_data(stores_data_endpoint, headers, stores_total)
    df_clean_stores = datcl.clean_store_data(stores_data)
    # Upload dataframe to database as table
    datex.upload_to_db(datco, df_clean_stores, 'dim_store_details')

    ## PRODUCT DATA
    # Read product data
    s3_product_data_path = 's3://data-handling-public/products.csv'
    prod_dataframe = datex.extract_from_s3(s3_product_data_path)
    df_clean_prod = datcl.clean_products_data(prod_dataframe)
    df_clean_prod = datcl.convert_product_weights(df_clean_prod)
    datex.upload_to_db(datco, df_clean_prod, 'dim_products')

    ## ORDERS DATA
    # Read orders data
    df_orders_table = datex.read_rds_table(datco, 'orders_table')
    df_clean_orders = datcl.clean_orders_data(df_orders_table)
    # Upload dataframe to database as table
    datex.upload_to_db(datco, df_clean_orders, 'orders_table')

    ## DATE AND TIME DATA
    # Read date_details .json
    json_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_details = datex.retrieve_json_data(json_path)
    df_clean_date = datcl.clean_date_time_data(date_details)
    # Upload dataframe to database as table
    datex.upload_to_db(datco, df_clean_date, 'dim_date_times')
