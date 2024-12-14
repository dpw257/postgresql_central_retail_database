from database_utils import DatabaseConnector


'''
SQL QUERIES
Uses SQL to defines data types of columns in each table in local database and prints lists of column names and data types.
Defines primary and foreign keys to enable starbased schema for the database.
Adds, defines and populates columns for booleans and other columns of derived data to facilitate subsequent SQL queries.
'''

# Connect to the database
datco = DatabaseConnector('db_creds.yaml')

# ORDERS TABLE
datco.connect_sql_database("ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid", True)
datco.connect_sql_database("ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid", True)
datco.connect_sql_database("ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT", True)
datco.connect_sql_database("ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(12)", True)
datco.connect_sql_database("ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR(12)", True)
datco.connect_sql_database("ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(19)", True)
datco.connect_sql_database("SELECT column_name, DATA_TYPE FROM information_schema.columns where table_name = 'orders_table'")
print('\n')

## USERS TABLE
datco.connect_sql_database("ALTER TABLE dim_users ALTER COLUMN first_name TYPE VARCHAR(20)", True)
datco.connect_sql_database("ALTER TABLE dim_users ALTER COLUMN last_name TYPE VARCHAR(20)", True)
datco.connect_sql_database("ALTER TABLE dim_users ALTER COLUMN date_of_birth TYPE DATE", True)
datco.connect_sql_database("ALTER TABLE dim_users ALTER COLUMN country_code TYPE VARCHAR(2)", True)
datco.connect_sql_database("ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid", True)
datco.connect_sql_database("ALTER TABLE dim_users ALTER COLUMN join_date TYPE DATE", True)
datco.connect_sql_database("SELECT column_name, DATA_TYPE FROM information_schema.columns where table_name = 'dim_users'")
print('\n')

## STORE DETAILS TABLE
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN locality TYPE VARCHAR(25)", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE VARCHAR(12)", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN opening_date TYPE DATE", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN store_type TYPE VARCHAR(11)", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN country_code TYPE VARCHAR(2)", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN continent TYPE VARCHAR(7)", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT", True)
datco.connect_sql_database("SELECT column_name, DATA_TYPE FROM information_schema.columns where table_name = 'dim_store_details'")
datco.connect_sql_database("UPDATE dim_store_details SET locality = 'N\A' WHERE store_code LIKE 'WEB-1388012W'", True)
# Add online/offline column to dim_store_details
datco.connect_sql_database("ALTER TABLE dim_store_details ADD store_location VARCHAR(8)", True) # ADD COLUMN
datco.connect_sql_database("UPDATE dim_store_details SET store_location = 'Offline' WHERE store_type != 'Web Portal'", True)
datco.connect_sql_database("UPDATE dim_store_details SET store_location = 'Web' WHERE store_type = 'Web Portal'", True)
print('\n')

## PRODUCTS TABLE
# Remove £-sign from prices
datco.connect_sql_database("UPDATE dim_products SET product_price = LTRIM(product_price, '£')", True) # ADD COLUMN
# Define human-readable labels for weight bins
datco.connect_sql_database("ALTER TABLE dim_products ADD weight_class VARCHAR(14)", True) # ADD COLUMN
datco.connect_sql_database("UPDATE dim_products SET weight_class = 'Light' WHERE weight < 200", True)
datco.connect_sql_database("UPDATE dim_products SET weight_class = 'Mid_Sized' WHERE weight BETWEEN 200 AND 4000", True)
datco.connect_sql_database("UPDATE dim_products SET weight_class = 'Heavy' WHERE weight BETWEEN 4000 AND 14000", True)
datco.connect_sql_database("UPDATE dim_products SET weight_class = 'Truck_Required' WHERE weight >= 14000", True)
# Add boolean column still_available based on 'removed' column
datco.connect_sql_database("ALTER TABLE dim_products ADD still_available BOOL", True) # ADD COLUMN
datco.connect_sql_database("UPDATE dim_products SET still_available = TRUE WHERE removed != 'Removed'", True)
datco.connect_sql_database("UPDATE dim_products SET still_available = FALSE WHERE removed = 'Removed'", True)
# Define data types of columns
datco.connect_sql_database("ALTER TABLE dim_products ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT", True)
datco.connect_sql_database("ALTER TABLE dim_products ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT", True)
datco.connect_sql_database("ALTER TABLE dim_products ALTER COLUMN product_code TYPE VARCHAR(11)", True)
datco.connect_sql_database("ALTER TABLE dim_products ALTER COLUMN date_added TYPE DATE", True)
datco.connect_sql_database("ALTER TABLE dim_products ALTER COLUMN uuid TYPE uuid USING uuid::uuid", True)
datco.connect_sql_database("ALTER TABLE dim_products ALTER COLUMN ean TYPE VARCHAR(17)", True)
datco.connect_sql_database("SELECT column_name, DATA_TYPE FROM information_schema.columns where table_name = 'dim_products'")
print('\n')

## DATE/TIME TABLE
datco.connect_sql_database("ALTER TABLE dim_date_times ALTER COLUMN year TYPE VARCHAR(4)", True)
datco.connect_sql_database("ALTER TABLE dim_date_times ALTER COLUMN month TYPE VARCHAR(2)", True)
datco.connect_sql_database("ALTER TABLE dim_date_times ALTER COLUMN day TYPE VARCHAR(2)", True)
datco.connect_sql_database("ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE VARCHAR(10)", True)
datco.connect_sql_database("ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid", True)
datco.connect_sql_database("SELECT column_name, DATA_TYPE FROM information_schema.columns where table_name = 'dim_date_times'")
# Add columns for leapyear boolean, calendar days in previous month, and timestamp in seconds
datco.connect_sql_database("ALTER TABLE dim_date_times ADD month_days INTEGER", True) # ADD COLUMN
datco.connect_sql_database("ALTER TABLE dim_date_times ADD leapyear BOOL", True) # ADD COLUMN
datco.connect_sql_database("UPDATE dim_date_times SET leapyear = FALSE", True)
datco.connect_sql_database("UPDATE dim_date_times SET leapyear = TRUE WHERE CAST(year AS INTEGER)%4 = 0", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 0 WHERE month = '1'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 31 WHERE month = '2'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 59 WHERE month = '3'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 60 WHERE month = '3' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 90 WHERE month = '4'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 91 WHERE month = '4' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 120 WHERE month = '5'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 121 WHERE month = '5' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 151 WHERE month = '6'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 152 WHERE month = '6' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 181 WHERE month = '7'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 182 WHERE month = '7' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 212 WHERE month = '8'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 213 WHERE month = '8' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 243 WHERE month = '9'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 244 WHERE month = '9' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 273 WHERE month = '10'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 274 WHERE month = '10' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 304 WHERE month = '11'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 305 WHERE month = '11' AND leapyear = TRUE", True) 
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 334 WHERE month = '12'", True)
datco.connect_sql_database("UPDATE dim_date_times SET month_days = 335 WHERE month = '12' AND leapyear = TRUE", True) 
datco.connect_sql_database("ALTER TABLE dim_date_times ADD seconds_stamp INTEGER", True) # ADD COLUMN
datco.connect_sql_database("UPDATE dim_date_times SET seconds_stamp = ((CAST(year AS INTEGER)-1992) * 365.25 + month_days + (CAST(day AS INTEGER)-1))*24*3600 + CAST(LEFT(timestamp,2) AS INTEGER)*3600 + CAST(SUBSTRING(timestamp, 4,2) AS INTEGER)*60 + CAST(SUBSTRING(timestamp, 7,2) AS INTEGER)", True)
print('\n')

## CARD DETAILS TABLE
datco.connect_sql_database("ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(19)", True)
datco.connect_sql_database("ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE VARCHAR(5)", True)
datco.connect_sql_database("ALTER TABLE dim_card_details ALTER COLUMN date_payment_confirmed TYPE DATE", True)
datco.connect_sql_database("SELECT column_name, DATA_TYPE FROM information_schema.columns where table_name = 'dim_card_details'")
print('\n'*2)

## DEFINE PRIMARY KEYS
datco.connect_sql_database("ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid)", True)
datco.connect_sql_database("ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code)", True)
datco.connect_sql_database("ALTER TABLE dim_products ADD PRIMARY KEY (product_code)", True)
datco.connect_sql_database("ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid)", True)
datco.connect_sql_database("ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number)", True)

## DEFINE FOREIGN KEYS
datco.connect_sql_database("ALTER TABLE orders_table ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid)", True)
datco.connect_sql_database("ALTER TABLE orders_table ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code)", True)
datco.connect_sql_database("ALTER TABLE orders_table ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code)", True)
datco.connect_sql_database("ALTER TABLE orders_table ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid)", True)
datco.connect_sql_database("ALTER TABLE orders_table ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number)", True)

print("### SQL star-based database schema complete. ###", '\n'*2, '----'*30, '\n'*3)
