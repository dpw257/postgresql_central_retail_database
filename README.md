# Centralised Database for Multinational Retail Data

## Project overview
The aim of the project was to consolidate data from various sources into a single database for easier access and analysis. Tasks included extracting and cleaning retail data from multiple platforms and file formats, such as S3 buckets, AWS RDS and PDFs using Boto3. After extraction, the data was cleaned to using custom functions and stored as Pandas dataframes. The cleaned dataframes were then saved to a local PostgreSQL database following a star-based schema. SQL queries were run with Python and Spark to generate insights and reports from the database.


---

## Table of contents

- [Project overview](#project-overview)
- [Usage instructions](#usage-instructions)
- [SQL queries](#sql-queries)
- [License](#license)



---

## Usage instructions

### 1. Set up server credentials
A YAML file named `db_creds.yaml` with credentials is required to extract the data files from the AWS server. The YAML file also contains credentials for accessing the local SQL database:

   ```yaml
   aws:
     access_key: YOUR_AWS_ACCESS_KEY
     secret_key: YOUR_AWS_SECRET_KEY
     region: YOUR_AWS_REGION

   db:
     host: YOUR_LOCAL_DB_HOST
     port: 5432
     user: YOUR_DB_USER
     password: YOUR_DB_PASSWORD
     database: YOUR_DB_NAME
   ```

### 2. Extract, clean and save data
**2a. Run script**
Download files and run `code_main.py` to extract data from AWS servers, clean it and save output to a local SQL database (PostgreSQL), which can be accessed using the credentials provided in `db_creds.yaml`.


**2b. Database schema**
The database follows a star-schema with the following tables:
- orders_table (fact table)
- dim_users
- dim_store_details
- dim_products
- dim_date_times
- dim_card_details

---

## SQL Queries

You can execute SQL queries to extract insights from the database by modifying the `sql_queries.py` file. This file contains predefined queries, but you can add your own queries to interact with the database as needed.

The files should be run in the following order:
- `main_code.py`: To extract, clean and save the data
- `sql_starbased.py`: To create a starbased schema for the database, add further columns and define data types in columns
- `sql_queries.py`: To run defined SQL queries on data


---

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE - see the [LICENSE](LICENSE.txt) file for details.

Copyright (c) [2024] [Daniel White]
