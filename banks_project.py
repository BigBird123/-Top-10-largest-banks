# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime 


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("code_log.txt","a") as f: 
        f.write(timestamp + ':' + message + '\n') 

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    table = soup.find('span', string=table_attribs).find_next('table')
    df = pd.read_html(str(table))[0]

    log_progress("Data extraction complete. Initiating Transformation process")
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path).set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['Market cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['Market cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['Market cap (US$ billion)']]
   
    log_progress('Data transformation complete. Initiating Loading process')
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)
    
    log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    log_progress("SQL Connection initiated")

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

    log_progress("Data loaded to Database as a table, Executing queries")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
 
    output = pd.read_sql(query_statement, sql_connection)

    log_progress("Process Complete")
    return output

if __name__ == "__main__":
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attribs = "By market capitalization"
    csv_path = "exchange_rate.csv"
    output_csv = "./Largest_banks_data.csv"

    database_name = "Banks.db"
    table_name = "Largest_banks"    
    sql_connection = sqlite3.connect(database_name)

    query1 = f'SELECT * FROM {table_name}'
    query2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
    query3 = f'SELECT "Bank name" from {table_name} LIMIT 5'

    log_progress("Preliminaries complete. Initiating ETL process")

    df = extract(url,table_attribs)
    transform_df = transform(df,csv_path)
    print(transform_df)
    load_to_csv(transform_df,output_csv)
    load_to_db(transform_df,sql_connection,table_name)
    print(run_query(query1,sql_connection))
    print(run_query(query2,sql_connection))
    print(run_query(query3,sql_connection))
   
    sql_connection.close()
    log_progress("Server Connection closed")
