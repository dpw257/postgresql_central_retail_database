# Centralised Database for Multinational Retail Data

## Project overview
The goal of this project was to consolidate data from various sources into a single database for easier access and analysis. This project focuses on the extraction, cleaning, and centralisation of retail data from multiple file formats into a local SQL database, such as AWS servers and PDFs. After extraction, the data is cleaned to address specific issues using custom functions and stored as Pandas dataframes. These cleaned dataframes are then saved to a PostgreSQL database following a star-based schema. SQL queries are executed using Python to generate insights and reports from the database.


---

## Table of contents

- [Project overview](#project-overview) 
- [Key technologies](#key-technologies) 
- [Installation instructions](#setup-instructions) 
- [Database schema](#database-schema)
- [SQL queries](#sql-queries)
- [License](#license)


---

## Key technologies

- **Python**: Data extraction, cleaning, and SQL query execution
- **Pandas**: Data manipulation and cleaning into dataframes
- **Boto3**: Accessing AWS S3 buckets/RDS
- **SQL/PostgresSQL** Areating a relational database, performing queries, storing in a local database

---

## Installation instructions
Download files and run code_main.py to extract, clean and save the data to a local SQL database (PostgreSQL).
A YAML file named db_creds.yaml with credentials is also required to extract the data files from the AWS server. The YAML file also contains credentials for accessing the local SQL database.

### Prerequisites
1. **Python 3.x**: Ensure that Python is installed on your local machine.
2. **PostgreSQL**: The project uses PostgreSQL as the local database.
3. **AWS Credentials**: A valid AWS account and access to the data stored on AWS.
4. **YAML file**: A `db_creds.yaml` file containing credentials for both AWS and the local PostgreSQL database.

### Steps to Run the Project

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/multinational-retail-data-centralisation.git
   cd multinational-retail-data-centralisation
   ```

2. **Install Dependencies**
   Ensure you have all the necessary Python dependencies. You can install them via `pip`:
   ```bash
   pip install -r requirements.txt
   ```

3. **Setup AWS Credentials**
   Create a `db_creds.yaml` file in the root directory with the following structure:
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

4. **Run the Script**
   The main script `code_main.py` handles the extraction, cleaning, and saving of data to the PostgreSQL database. Run the following command to execute the process:
   ```bash
   python code_main.py
   ```

   This will:
   - Extract the data from AWS.
   - Clean the data using predefined functions.
   - Save the cleaned data into your local PostgreSQL database.

5. **Access the Database**
   Once the script completes, the data will be stored in your PostgreSQL database. You can access it using any PostgreSQL client (e.g., pgAdmin or psql) using the credentials provided in `db_creds.yaml`.


---

## Database schema

The database follows a **star schema** design with the following key tables:

- **Fact Table**: Contains the central data of interest, such as sales transactions.
- **Dimension Tables**: Contain descriptive data, such as product information, customer details, time data, etc.

### Example Tables:
- `sales_fact`: Stores transaction-level data (e.g., sales amounts, quantities sold).
- `products_dim`: Stores product details (e.g., product name, category).
- `customers_dim`: Stores customer information (e.g., name, location).
- `time_dim`: Stores time-related data (e.g., date, month, year).

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
