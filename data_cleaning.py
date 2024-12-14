import re
import pandas as pd
import datetime


class DataCleaning:
    '''
    CLASS OF DATA CLEARNING METHODS
    Defines class of methods for cleaning dataframes based on their column names and the specific issues found within them.
    Attributes: 
        All methods take Pandas dataframes as arguments
    '''
    #  Function to remove symbols from phone numbers as initial clearning step for USER DATA
    def remove_phone_symbols(self, dataframe):
        dataframe['phone_updated'] = 0
        for index in dataframe.index:
            if '(0)' in dataframe['phone_number'][index]:
                split_number = re.split(r'\(0\)', dataframe['phone_number'][index])
                updated_number = ''.join(split_number)
                dataframe.at[index, 'phone_number'] = updated_number
                dataframe.at[index, 'phone_updated'] = 1
            if ' ' in dataframe['phone_number'][index]:
                split_number = re.split(r' ', dataframe['phone_number'][index])
                updated_number = ''.join(split_number)
                dataframe.at[index, 'phone_number'] = updated_number
                dataframe.at[index, 'phone_updated'] = 1
            if '(' in dataframe['phone_number'][index]:
                split_number = re.split(r'\(', dataframe['phone_number'][index])
                updated_number = ''.join(split_number)
                dataframe.at[index, 'phone_number'] = updated_number
                dataframe.at[index, 'phone_updated'] = 1
            if ')' in dataframe['phone_number'][index]:
                split_number = re.split(r'\)', dataframe['phone_number'][index])
                updated_number = ''.join(split_number)
                dataframe.at[index, 'phone_number'] = updated_number
                dataframe.at[index, 'phone_updated'] = 1
            if '-' in dataframe['phone_number'][index]:
                split_number = re.split(r'-', dataframe['phone_number'][index])
                updated_number = ''.join(split_number)
                dataframe.at[index, 'phone_number'] = updated_number
                dataframe.at[index, 'phone_updated'] = 1
            if '.' in dataframe['phone_number'][index]:
                split_number = re.split(r'\.', dataframe['phone_number'][index])
                updated_number = ''.join(split_number)
                dataframe.at[index, 'phone_number'] = updated_number
                dataframe.at[index, 'phone_updated'] = 1
        return dataframe
    
    # Clean USER data as dataframe
    def clean_user_data(self, dataframe):
        # Count initial rows in table
        start_length = len(dataframe)
        print("Start length:", start_length, 'rows')
              
        # Remove rows with ERRORS or NULL
        errors_missing = 0
        errors_null = 0
        for index in dataframe.index:
            if dataframe['first_name'][index].isalpha() == False and \
                (' ' not in dataframe['first_name'][index]) and ("'" not in dataframe['first_name'][index]) and \
                ('-' not in dataframe['first_name'][index]):
                dataframe.at[index,'delete_row']='delete'
                errors_missing+=1
            elif dataframe['first_name'][index] == 'NULL':
                dataframe.at[index,'delete_row']='delete'
                errors_null+=1
        # Remove rows in delete_rows with value 'delete', then drop column
        dataframe = dataframe.loc[dataframe['delete_row'] != 'delete']
        dataframe = dataframe.drop('delete_row', axis=1)
        print("Rows removed - ERRORS:", errors_missing)
        print("Rows removed - NULL:", errors_null)
        
        # Remove duplicate rows
        prior_length = len(dataframe)
        dataframe['duplicated'] = dataframe.duplicated(subset=['first_name', 'last_name', 'phone_number'])
        for index in dataframe.index:
            if dataframe['duplicated'][index] == True:
                print(dataframe['first_name'][index], dataframe['last_name'][index])
        dataframe = dataframe.loc[dataframe['duplicated'] != True]
        dataframe = dataframe.drop('duplicated', axis=1)
        print('Rows removed - DUPLICATED:', prior_length-len(dataframe))
        print("Final length:", len(dataframe), 'rows', '\n')

        # Check first and last names
        errors_first_name = 0
        errors_last_name = 0
        for index in dataframe.index:
            if dataframe['first_name'][index].isalpha() == False and \
                (' ' not in dataframe['first_name'][index]) and ("'" not in dataframe['first_name'][index]) and \
                ('-' not in dataframe['first_name'][index]):
                errors_first_name+=1
            elif dataframe['first_name'][index] == 'NULL':
                errors_first_name+=1
            elif dataframe['first_name'][index][1].isupper():
                errors_first_name+=1
        errors_last_name = 0
        for index in dataframe.index:
            if dataframe['last_name'][index].isalpha() == False and \
                (' ' not in dataframe['last_name'][index]) and ("'" not in dataframe['last_name'][index]) and \
                ('-' not in dataframe['last_name'][index]):
                errors_last_name+=1
            elif dataframe['last_name'][index] == 'NULL':
                errors_last_name+=1
            elif dataframe['last_name'][index][1].isupper():
                errors_first_name+=1

        # Remove double @-sign and fix 'ä' in email_addresses
        count_emails = 0
        email_regex = r'^[A-Za-z0-9._-]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,4}){1,2}$'
        for index in dataframe.index:
            if '@@' in dataframe['email_address'][index]:
                split_email = re.split(r'@', dataframe['email_address'][index], maxsplit=1)
                updated_email = ''.join(split_email)
                dataframe.at[index, 'email_address'] = updated_email
                count_emails+=1
            if 'ä' in dataframe['email_address'][index]:
                split_email = re.split(r'ä', dataframe['email_address'][index], maxsplit=1)
                updated_email = 'ae'.join(split_email)
                dataframe.at[index, 'email_address'] = updated_email
                count_emails+=1
        print("Fixed emails:", count_emails)

        # Fix error in country code
        count_ccode = 0
        country_codes = []
        for index in dataframe.index:
            if dataframe.at[index, 'country_code'] == 'GGB':
                dataframe.at[index, 'country_code'] = 'GB'
                count_ccode+=1
            elif dataframe['country_code'][index] not in country_codes:
                country_codes.append(dataframe['country_code'][index])
        print("Fixed country codes:", count_ccode)


        # Fix phone numbers
        date_regex = r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        dataframe = self.remove_phone_symbols(dataframe)
        for index in dataframe.index:
            if dataframe['country_code'][index] == 'GB':
                dataframe.at[index, 'phone_number'] = re.sub(r'\b0', '+44', dataframe['phone_number'][index])
                dataframe.at[index, 'phone_updated'] = 1
            if dataframe['country_code'][index] == 'DE':
                dataframe.at[index, 'phone_number'] = re.sub(r'\b0', '+49', dataframe['phone_number'][index])
                dataframe.at[index, 'phone_updated'] = 1
            if dataframe['country_code'][index] == 'US':
                if dataframe['phone_number'][index][0] == '0' and dataframe['phone_number'][index][1] == '0' and dataframe['phone_number'][index][2] == '1':
                    dataframe.at[index, 'phone_number'] = re.sub(r'\b001', '+1', dataframe['phone_number'][index])
                    dataframe.at[index, 'phone_updated'] = 1
                if dataframe['phone_number'][index][0] != '+':
                    dataframe.at[index, 'phone_number'] = '+1' + dataframe['phone_number'][index]
                    dataframe.at[index, 'phone_updated'] = 1
        count_phones = dataframe['phone_updated'].sum()
        print("Fixed phone numbers:", count_phones)

        # Check user_uuid format
        count_uuid = 0
        uuid_regex = r'^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$'
        for index in dataframe.index:
            if not re.fullmatch(uuid_regex, dataframe['user_uuid'][index]):
                print(dataframe['user_uuid'][index])
                count_uuid+=1

        # Check dates format with REGEX
        count_join_date = 0
        count_birth_date = 0
        calendar = {'January':'01', 'February':'02', 'March':'03', 'April':'04', 'May':'05', 'June':'06', 'July':'07', 'August':'08', 'September':'09', 'October':'10', 'November':'11', 'December':'12'}
        date_regex = r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        for index in dataframe.index:
            if not re.fullmatch(date_regex, dataframe['join_date'][index]):
                if '/' in dataframe['join_date'][index]:
                    dataframe.at[index, 'join_date'] = re.sub(r'/', '-', dataframe['join_date'][index])
                    count_join_date+=1
                if ' ' in dataframe['join_date'][index]:
                    split_date = re.split(r' ', dataframe['join_date'][index], maxsplit=2)
                    for fig in range(len(split_date)):
                        if len(split_date[fig]) == 2:
                            split_date[fig]='00'+split_date[fig]
                    split_date = sorted(split_date)
                    date_list = []
                    date_list.append(split_date[1]) # Year
                    date_list.append(calendar[split_date[2]]) # Month
                    date_list.append(split_date[0][-2]+split_date[0][-1]) # Day
                    updated_date = '-'.join(date_list)
                    dataframe.at[index, 'join_date'] = updated_date
                    count_join_date+=1
            if not re.fullmatch(date_regex, dataframe['date_of_birth'][index]):
                if '/' in dataframe['date_of_birth'][index]:
                    dataframe.at[index, 'date_of_birth'] = re.sub(r'/', '-', dataframe['date_of_birth'][index])
                    count_birth_date+=1
                if ' ' in dataframe['date_of_birth'][index]:
                    split_date = re.split(r' ', dataframe['date_of_birth'][index], maxsplit=2)
                    for fig in range(len(split_date)):
                        if len(split_date[fig]) == 2:
                            split_date[fig]='00'+split_date[fig]
                    split_date = sorted(split_date)
                    date_list = []
                    date_list.append(split_date[1]) # Year
                    date_list.append(calendar[split_date[2]]) # Month
                    date_list.append(split_date[0][-2]+split_date[0][-1]) # Day
                    updated_date = '-'.join(date_list)
                    dataframe.at[index, 'date_of_birth'] = updated_date
                    count_birth_date+=1
        # Change date format to timestamp
        dataframe['date_of_birth'] = pd.to_datetime(dataframe['date_of_birth'], errors = 'coerce')
        dataframe['join_date'] = pd.to_datetime(dataframe['join_date'], errors ='coerce')
        dataframe = dataframe.dropna(subset=['date_of_birth'])
        dataframe = dataframe.dropna(subset=['join_date'])
        for index in dataframe.index:
            if not isinstance(dataframe['join_date'][index], datetime.datetime):
                print(type(dataframe['join_date'][index]), type(dataframe['date_of_birth'][index]), '\n')
        print("Fixed join_date:", count_join_date)
        print("Fixed date_of_birth:", count_birth_date)
        

        # Check countries
        countries = []
        for index in dataframe.index:
            if dataframe['country'][index] not in countries:
                countries.append(dataframe['country'][index])
        print("\n Countries:", countries, "\n Country codes:", country_codes, '\n')
        return dataframe


    # Clean CREDIT CARD data - remove rows with erroneous values, NULL values or errors with formatting
    def clean_card_data(self, card_dataframe):
        start_length = len(card_dataframe)
        print("Start length:", start_length, 'rows')
        # Clean expiry dates
        errors_missing = 0
        errors_null = 0
        expiry_regex = r'^[0-9]{2}/[0-9]{2}$'
        for index in card_dataframe.index:
            if card_dataframe['expiry_date'][index] == 'NULL':
                card_dataframe.at[index,'delete_row']='delete'
                errors_null+=1
            elif not re.fullmatch(expiry_regex, card_dataframe['expiry_date'][index]):
                card_dataframe.at[index,'delete_row']='delete'
                errors_missing+=1
        card_dataframe = card_dataframe.loc[card_dataframe['delete_row'] != 'delete']
        card_dataframe = card_dataframe.drop('delete_row', axis=1)
        print("Rows removed - ERRORS:", errors_missing)
        print("Rows removed - NULL:", errors_null, '\n')

        # Check format of date_payment_confirmed with REGEX
        count_date_payment_confirmed = 0
        calendar = {'January':'01', 'February':'02', 'March':'03', 'April':'04', 'May':'05', 'June':'06', 'July':'07', 'August':'08', 'September':'09', 'October':'10', 'November':'11', 'December':'12'}
        date_regex = r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        for index in card_dataframe.index:
            if not re.fullmatch(date_regex, card_dataframe['date_payment_confirmed'][index]):
                if '/' in card_dataframe['date_payment_confirmed'][index]:
                    card_dataframe.at[index, 'date_payment_confirmed'] = re.sub(r'/', '-', card_dataframe['date_payment_confirmed'][index])
                    count_date_payment_confirmed+=1
                if ' ' in card_dataframe['date_payment_confirmed'][index]:
                    split_date = re.split(r' ', card_dataframe['date_payment_confirmed'][index], maxsplit=2)
                    for fig in range(len(split_date)):
                        if len(split_date[fig]) == 2:
                            split_date[fig]='00'+split_date[fig]
                    split_date = sorted(split_date)
                    date_list = []
                    date_list.append(split_date[1]) # Year
                    date_list.append(calendar[split_date[2]]) # Month
                    date_list.append(split_date[0][-2]+split_date[0][-1]) # Day
                    updated_date = '-'.join(date_list)
                    card_dataframe.at[index, 'date_payment_confirmed'] = updated_date
                    count_date_payment_confirmed+=1
        # Change date format to timestamp
        card_dataframe['date_payment_confirmed'] = pd.to_datetime(card_dataframe['date_payment_confirmed'], errors ='coerce')
        card_dataframe = card_dataframe.dropna(subset=['date_payment_confirmed'])
        for index in card_dataframe.index:
            if not isinstance(card_dataframe['date_payment_confirmed'][index], datetime.datetime):
                print(type(card_dataframe['date_payment_confirmed'][index]), '\n')
        print("Fixed date_payment:", count_date_payment_confirmed)

        # Clean card_numbers
        count_card_numbers = 0
        for index in card_dataframe.index:
            if type(card_dataframe['card_number'][index]) != int:
                if '?' in card_dataframe['card_number'][index]:
                    split_card_number = re.split(r'\?', card_dataframe['card_number'][index], maxsplit=5)
                    updated_card_number = ''.join(split_card_number)
                    card_dataframe.at[index, 'card_number'] = updated_card_number
                    count_card_numbers+=1
        print("Fixed card_numbers:", count_card_numbers)

        # Clean card_providers 
        card_providers = []
        prior_length = len(card_dataframe)
        for index in card_dataframe.index:
            if card_dataframe['card_provider'][index] not in card_providers:
                card_providers.append(card_dataframe['card_provider'][index])
        card_providers = [('Diners Club / Carte Blanche', 14), ('American Express', 15), ('JCB 16 digit', 16), ('JCB 15 digit', 15), ('Maestro', 12), ('Mastercard', 16), ('Discover', 16), ('VISA 19 digit', 19), ('VISA 16 digit', 16),  ('VISA 13 digit', 13)]
        card_providers_DICT = {}
        errors_missing = 0
        for index in card_dataframe.index:
            if (card_dataframe['card_provider'][index], len(str(card_dataframe['card_number'][index]))) not in card_providers and \
                (card_dataframe['card_provider'][index], len(str(card_dataframe['card_number'][index]))) not in card_providers_DICT:
                card_dataframe.at[index,'delete_row']='delete'
                errors_missing+=1
                card_providers_DICT[(card_dataframe['card_provider'][index], len(str(card_dataframe['card_number'][index])))] = 1
            elif (card_dataframe['card_provider'][index], len(str(card_dataframe['card_number'][index]))) in card_providers_DICT:
                card_providers_DICT[(card_dataframe['card_provider'][index], len(str(card_dataframe['card_number'][index])))] += 1
                card_dataframe.at[index,'delete_row']='delete'
                errors_missing+=1
        if len(card_providers_DICT) > 0:
            print('\n')
            print('ERRORS IN LENGTH OF CARD NUMBERS:')
            for key in card_providers_DICT.keys():
                print(f' Found {card_providers_DICT[key]} {key[0]} cards with {key[1]} digits')             
        card_dataframe = card_dataframe.drop('delete_row', axis=1)
        print("CARD DIGIT ERRORS (not removed):", errors_missing)
        print('\n')

        # Remove duplicate rows:
        prior_length = len(card_dataframe)
        card_dataframe['duplicated'] = card_dataframe.duplicated(subset=['card_number'])
        for index in card_dataframe.index:
            if card_dataframe['duplicated'][index] == True:
                print(card_dataframe['card_number'][index])
        card_dataframe = card_dataframe.loc[card_dataframe['duplicated'] != True]
        card_dataframe = card_dataframe.drop('duplicated', axis=1)
        print('Rows removed - DUPLICATED:', prior_length-len(card_dataframe))
        print('\n')

        print("Final length:", len(card_dataframe), 'rows', '\n')
        print('\n')
        df_clean_card = card_dataframe
        return df_clean_card
    
    # Clean STORES DATA as DataFrame.
    def clean_store_data(self, stores_dataframe):
        # Drop extra column 'lat'
        stores_dataframe = stores_dataframe.drop('lat', axis=1)

        # Remove rows with errors or null in country_code
        errors_missing = 0
        for index in stores_dataframe.index:
            if stores_dataframe['address'][index] == 'N/A':
                stores_dataframe.at[index,'address']=None
                stores_dataframe.at[index,'longitude']=None
                stores_dataframe.at[index,'latitude']=None
            if len(str(stores_dataframe['country_code'][index])) != 2:
                stores_dataframe.at[index,'delete_row']='delete'
                errors_missing+=1
        stores_dataframe = stores_dataframe.loc[stores_dataframe['delete_row'] != 'delete']
        stores_dataframe = stores_dataframe.drop('delete_row', axis=1)
        print("Rows removed - ERRORS and NULL:", errors_missing, '\n')

        # Check country_codes are correct
        country_codes = []
        for index in stores_dataframe.index:
            if stores_dataframe['country_code'][index] not in country_codes:
                country_codes.append(stores_dataframe['country_code'][index])

        # Check store_types are correct
        store_types = []
        for index in stores_dataframe.index:
            if stores_dataframe['store_type'][index] not in store_types:
                store_types.append(stores_dataframe['store_type'][index])

        # Clean staff_numbers by removing letters with REGEX
        count_staff_errors = 0
        staff_regex = r'^[0-9]{1,3}$'
        for index in stores_dataframe.index:
            if not re.fullmatch(staff_regex, stores_dataframe['staff_numbers'][index]):
                split_staff = re.split(r'\D', stores_dataframe['staff_numbers'][index], maxsplit=1)
                updated_staff = ''.join(split_staff)
                stores_dataframe.at[index, 'staff_numbers'] = updated_staff
                count_staff_errors+=1
        print(f'Fixed staff numbers: {count_staff_errors}')


        # Fix typos in continents with REGEX
        count_continent_errors = 0
        for index in stores_dataframe.index:
            if 'ee' in stores_dataframe['continent'][index]:
                split_continent = re.split(r'ee', stores_dataframe['continent'][index], maxsplit=1)
                updated_continent = ''.join(split_continent)
                stores_dataframe.at[index, 'continent'] = updated_continent
                count_continent_errors+=1
        print(f'Fixed continents: {count_continent_errors}')
        continents = []
        for index in stores_dataframe.index:
            if stores_dataframe['continent'][index] not in continents:
                continents.append(stores_dataframe['continent'][index])
       

        # Check opening_date format with REGEX
        count_opening_date = 0
        calendar = {'January':'01', 'February':'02', 'March':'03', 'April':'04', 'May':'05', 'June':'06', 'July':'07', 'August':'08', 'September':'09', 'October':'10', 'November':'11', 'December':'12'}
        date_regex = r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        for index in stores_dataframe.index:
            if not re.fullmatch(date_regex, stores_dataframe['opening_date'][index]):
                if '/' in stores_dataframe['opening_date'][index]:
                    stores_dataframe.at[index, 'opening_date'] = re.sub(r'/', '-', stores_dataframe['opening_date'][index])
                    count_opening_date+=1
                if ' ' in stores_dataframe['opening_date'][index]:
                    split_date = re.split(r' ', stores_dataframe['opening_date'][index], maxsplit=2)
                    for fig in range(len(split_date)):
                        if len(split_date[fig]) == 2:
                            split_date[fig]='00'+split_date[fig]
                    split_date = sorted(split_date)
                    date_list = []
                    date_list.append(split_date[1]) # Year
                    date_list.append(calendar[split_date[2]]) # Month
                    date_list.append(split_date[0][-2]+split_date[0][-1]) # Day
                    updated_date = '-'.join(date_list)
                    stores_dataframe.at[index, 'opening_date'] = updated_date
                    count_opening_date+=1
        # Change date format to timestamp
        stores_dataframe['opening_date'] = pd.to_datetime(stores_dataframe['opening_date'], errors ='coerce')
        stores_dataframe = stores_dataframe.dropna(subset=['opening_date'])
        for index in stores_dataframe.index:
            if not isinstance(stores_dataframe['opening_date'][index], datetime.datetime):
                print(type(stores_dataframe['opening_date'][index]), '\n')
        print("Fixed opening_date:", count_opening_date, '\n')

        # Confirm no errors remaining in string values by printing
        print(continents)
        print(country_codes)
        print(store_types, '\n')
        return stores_dataframe


    # SUB-FUNCTION for cleaning PRODUCT data - Standardise, fix issues and remove weight units in DataFrame (g, ml, oz, kg)
    def convert_product_weights(self, prod_dataframe):
        errors_weights = 0
        for index in prod_dataframe.index:
            if prod_dataframe['weight'][index][-1] == 'g':
                updated_weight = prod_dataframe['weight'][index][:-1]
                prod_dataframe.at[index, 'weight'] = updated_weight
            if prod_dataframe['weight'][index][-1] == 'l':
                updated_weight = prod_dataframe['weight'][index][:-2]
                prod_dataframe.at[index, 'weight'] = updated_weight
                errors_weights+=1
            if prod_dataframe['weight'][index][-1] == 'z':
                updated_weight = prod_dataframe['weight'][index][:-2]
                prod_dataframe.at[index, 'weight'] = round(float(updated_weight)*28.3495, 2)
                errors_weights+=1
            if str(prod_dataframe['weight'][index])[-1] == '.':
                updated_weight = str(prod_dataframe['weight'][index])[:-3]
                prod_dataframe.at[index, 'weight'] = updated_weight
                errors_weights+=1
            if str(prod_dataframe['weight'][index])[-1] == 'k':
                updated_weight = str(prod_dataframe['weight'][index])[:-1]
                prod_dataframe.at[index, 'weight'] = float(updated_weight)*1000
                errors_weights+=1
            if ' x ' in str(prod_dataframe['weight'][index]):
                split_weight = re.split(r' x ', str(prod_dataframe['weight'][index]))
                updated_weight = float(split_weight[0])*float(split_weight[1])
                prod_dataframe.at[index, 'weight'] = updated_weight
                errors_weights+=1
        for index in prod_dataframe.index:
            prod_dataframe.at[index, 'weight'] = float(prod_dataframe.at[index, 'weight'])
        print('Fixed weights:', errors_weights, '\n')
        print("Final length:", len(prod_dataframe), 'rows', '\n')
        return prod_dataframe
    

    # Clean PRODUCT data into dataframe
    def clean_products_data(self, prod_dataframe):
        # Drop extra index column
        prod_dataframe = prod_dataframe.drop('Unnamed: 0', axis=1)
        # Remove NULL and error rows
        errors_null = 0
        last_chars = ['U', 'X', 'L', 'n']
        for index in prod_dataframe.index:
            if str(prod_dataframe['weight'][index])[-1] in last_chars:
                prod_dataframe.at[index,'delete_row']='delete'
                errors_null+=1
        prod_dataframe = prod_dataframe.rename(columns={'EAN': 'ean'})
        # Remove rows in delete_rows with value 'delete', then drop column
        prod_dataframe = prod_dataframe.loc[prod_dataframe['delete_row'] != 'delete']
        prod_dataframe = prod_dataframe.drop('delete_row', axis=1)
        print("Rows removed - NULL and ERRORS:", errors_null, '\n')

        # Check date_added format with REGEX
        count_date_added = 0
        calendar = {'January':'01', 'February':'02', 'March':'03', 'April':'04', 'May':'05', 'June':'06', 'July':'07', 'August':'08', 'September':'09', 'October':'10', 'November':'11', 'December':'12'}
        date_regex = r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        for index in prod_dataframe.index:
            if not re.fullmatch(date_regex, prod_dataframe['date_added'][index]):
                if '/' in prod_dataframe['date_added'][index]:
                    prod_dataframe.at[index, 'date_added'] = re.sub(r'/', '-', prod_dataframe['date_added'][index])
                    count_date_added+=1
                if ' ' in prod_dataframe['date_added'][index]:
                    split_date = re.split(r' ', prod_dataframe['date_added'][index], maxsplit=2)
                    for fig in range(len(split_date)):
                        if len(split_date[fig]) == 2:
                            split_date[fig]='00'+split_date[fig]
                    split_date = sorted(split_date)
                    date_list = []
                    date_list.append(split_date[1]) # Year
                    date_list.append(calendar[split_date[2]]) # Month
                    date_list.append(split_date[0][-2]+split_date[0][-1]) # Day
                    updated_date = '-'.join(date_list)
                    prod_dataframe.at[index, 'date_added'] = updated_date
                    count_date_added+=1
        # Change date format to timestamp
        prod_dataframe['date_added'] = pd.to_datetime(prod_dataframe['date_added'], errors ='coerce')
        prod_dataframe = prod_dataframe.dropna(subset=['date_added'])
        for index in prod_dataframe.index:
            if not isinstance(prod_dataframe['date_added'][index], datetime.datetime):
                print(type(prod_dataframe['date_added'][index]))
        print("Fixed date_added:", count_date_added)
        return prod_dataframe

    # Clean ORDERS data as dataframe
    def clean_orders_data(self, order_dataframe):
        columns_to_drop = ['first_name', 'last_name', '1', 'level_0', 'index']
        order_dataframe = order_dataframe.drop(columns_to_drop, axis=1)
        print(f'Dropped {len(columns_to_drop)} columns: {columns_to_drop} \n')
        return order_dataframe

    # Clean DATE AND TIME data as dataframe
    def clean_date_time_data(self, date_dataframe):
        errors_null = 0
        for index in date_dataframe.index:
            if len(date_dataframe['month'][index]) > 2:
                date_dataframe.at[index,'delete_row']='delete'
                errors_null+=1
        date_dataframe = date_dataframe.loc[date_dataframe['delete_row'] != 'delete']
        date_dataframe = date_dataframe.drop('delete_row', axis=1)
        print("Rows removed - NULL and ERRORS:", errors_null, '\n')
        print("Final length:", len(date_dataframe), 'rows', '\n')
        return date_dataframe
