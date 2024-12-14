from database_utils import DatabaseConnector


'''
Conducts SQL queries on local database and prints results.
'''

# Connect to the database
datco = DatabaseConnector('db_creds.yaml')
print('\n')

## Query 1: Stores per country
print('## Query 1: Stores per country')
datco.connect_sql_database("SELECT country_code, COUNT(country_code) AS store_count FROM dim_store_details \
                           GROUP BY country_code ORDER BY store_count DESC LIMIT 3")
print('\n')

## Query 2: Stores per locality
print('## Query 2: Stores per locality')
datco.connect_sql_database("SELECT locality, COUNT(locality) AS locality_count FROM dim_store_details GROUP BY locality \
                           ORDER BY locality_count DESC LIMIT 7")
print('\n'*2)

## Query 3: Total revenue per month
print('## Query 3: Total sales per month')
datco.connect_sql_database("SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price)*100)/100 AS total_sales, month \
                           FROM orders_table INNER JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid \
                           INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code GROUP BY month \
                           ORDER BY total_sales DESC LIMIT 6")
print('\n'*2)

## Query 4: Total sales per offline/online stores
print("## Query 4: Total sales per offline/online stores")
datco.connect_sql_database("SELECT store_location, COUNT(store_location) AS store_location_count FROM dim_store_details \
                           GROUP BY store_location ORDER BY store_location_count DESC LIMIT 7")
print('\n'*2)

## Query 5: Total/percentage revenue per store type
print("## Query 5: Total/percentage revnue per store type")
datco.connect_sql_database("SELECT store_type, ROUND(SUM(orders_table.product_quantity * dim_products.product_price)*100)/100 AS total_sales, \
                           ROUND(SUM(orders_table.product_quantity * dim_products.product_price)*10000/(SELECT SUM(orders_table.product_quantity * dim_products.product_price) \
                           FROM orders_table INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code))/100 FROM orders_table \
                           INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code INNER JOIN dim_store_details \
                           ON orders_table.store_code = dim_store_details.store_code GROUP BY store_type ORDER BY total_sales DESC")
print('\n'*2)

## Query 6: Top month/year for revenue 
print("## Query 6: Top month/year for revenue")
datco.connect_sql_database("SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price)*100)/100 \
                           AS total_sales, year, month FROM orders_table INNER JOIN dim_date_times \
                           ON orders_table.date_uuid = dim_date_times.date_uuid INNER JOIN dim_products \
                           ON orders_table.product_code = dim_products.product_code GROUP BY month, year \
                           ORDER BY total_sales DESC LIMIT 10")
print('\n'*2)

## Query 7: Staff per country
print("## Query 7: Staff per country")
datco.connect_sql_database("SELECT SUM(staff_numbers) AS total_staff_numbers, country_code FROM dim_store_details \
                           GROUP BY country_code ORDER BY total_staff_numbers DESC")
print('\n'*2)

## Query 8: Revenue per store type in Germany
print("## Query 8: Revenue per store type in Germany")
datco.connect_sql_database("SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price)*100)/100 \
                           AS total_sales, store_type, country_code FROM orders_table INNER JOIN dim_store_details \
                           ON orders_table.store_code = dim_store_details.store_code INNER JOIN dim_products \
                           ON orders_table.product_code = dim_products.product_code WHERE country_code = 'DE' \
                           GROUP BY store_type, country_code ORDER BY total_sales")
print('\n'*2)

## Query 9: Avg. speed of sales per year
print("## Query 9: Avg. speed of sales per year")
datco.connect_sql_database("SELECT year, ' hours:', (MAX(seconds_stamp)-MIN(seconds_stamp))/COUNT(seconds_stamp)/3600, ' minutes:', \
                           (MAX(seconds_stamp)-MIN(seconds_stamp))/COUNT(seconds_stamp)%3600/60, ' seconds:', \
                           (MAX(seconds_stamp)-MIN(seconds_stamp))/COUNT(seconds_stamp)%60 FROM dim_date_times GROUP BY year \
                           ORDER BY (MAX(seconds_stamp)-MIN(seconds_stamp))/COUNT(seconds_stamp) DESC LIMIT 5")
print('\n'*2)

print("### SQL queries complete. ###", '\n'*2, '--'*60, '\n'*3)
