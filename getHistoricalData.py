"""
Script for obtaining historical prices from Yahoo finance and uploading to mysql database.
This script has already been run on the full universe of stocks tracked by Yahoo, and the
database tables have been filled with data up to May 10, 2016.

TO DO: Add updating functionality to this script, to retrieve historical data up to current date.
"""

import time
import random

#import myql
import pandas as pd
from pandas.io.sql import SQLDatabase as pdsql
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Enum, Numeric, Date, Index
from pandas_datareader.data import DataReader

import DBEngine

price_table = 'histPrice'

def get_price_table(engine):
    conn = engine.connect()
    has_table = pdconn.has_table('histPrices')
    metadata = MetaData()

    if has_table is False:
        pt = Table(price_table, metadata,
                   Column('ticker', String(6), primary_key=True),
                   Column('date', Date(),primary_key=True),
                   Column('Open', Numeric(8,2)),
                   Column('High', Numeric(8,2)),
                   Column('Low', Numeric(8,2)),
                   Column('Close', Numeric(8,2)),
                   Column('Volume', Integer),
                   Column('AdjClose', Numeric(8,2)))

        metadata.create_all(engine)
    else:
        pt = Table('histPrice',meta,autoload_with=engine)
    return pt
    


engine = DBEngine.create_engine('stocks')  # get DB engine for 'stocks' database
pdconn = pdsql(engine)   # pandas.io.sql connection

histPrice = get_price_table(engine)

cursor = pdconn.execute('select distinct ticker from histPrice')
done_tuples = cursor.fetchall()
done = [x[0] for x in done_tuples]

#companies = pdconn.read_table('companies')
#cursor = pdconn.execute('select ticker from company c left join industry i on c.industry = i.industry where sector = "Healthcare"')
cursor = pdconn.execute('select ticker from company')
companies = cursor.fetchall()

start_date = '1985-01-01'
#start_date = '2016-01-01'
end_date = '2016-05-10'

#companies is a list of tuples
for co in companies:
    co = co[0]
    if co in done: continue
    done.append(co)
    print("Fetching data for "+co)
    #fetch data from yahoo as pandas dataframe
    try:
        ts = DataReader(co,'yahoo',start=start_date,end=end_date)
    except:
        print('Could not read data for '+co)
        continue
    ts.rename(columns={'Adj Close':'AdjClose'},inplace=True)
    # AdjClose values sometimes get extremely large, causing out of bounds errors.  Limit those values here.
    if max(ts['AdjClose']) > 9.99e5:
        ts.loc[ts.AdjClose > 9.99e5,'AdjClose'] = 9.99e5
    ts.insert(0,'ticker',co)  #add ticker to dataframe
    ts = ts.round(decimals=2)
    pdconn.to_sql(ts,'histPrice',if_exists='append')
    time.sleep(random.randint(1,5))


