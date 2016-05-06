"""
Script for uploading the FDA events table to the SQL database.

TO DO: dates get uploaded as strings.  Process dataframe so dates become dates.  
Or alter table after upload.
"""
    
import pandas as pd
from pandas.io.sql import SQLDatabase as pdsql

import DBEngine

source_file = '~/work/BioPharmaEvents/drug_event_db_v03.csv'

try:
    df = pd.read_csv(source_file)
except UnicodeDecodeError:
    df = pd.read_csv(source_file,encoding='latin-1')
except:
    print("Could not read source file")
    exit(1)

table_name = 'FDAEvents'

def build_events_table(frame, conn):
    pdconn.to_sql(frame, 'FDAEvents', if_exists='replace', index=False)

def read_events_table(conn):
    return conn.read_table('FDAEvents')

engine = DBEngine.create_engine('stocks')  # get DB engine for 'stocks' database
pdconn = pdsql(engine)   # pandas.io.sql connection
build_events_table(df, pdconn)
df2 = read_events_table(pdconn)  # for verification

#print(df2)

