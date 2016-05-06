"""
Script for setting up the sector, industry, and company tables in the 'stocks' database.  
This script reads data from yahoo URLs, creates the tables in the database, and uploads the info into
the tables. The script does a lot of page access to Yahoo, so it should be run rarely.
There is no provision yet for updating the data; instead the tables are wiped out and recreated.
"""

import re

#import myql  
import requests
from bs4 import BeautifulSoup
#import mechanize
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Enum

import DBEngine

#Yahoo industry sector page
base_url = 'https://biz.yahoo.com/p/'
sectors_url = 's_conameu.html'

def get_industry_data():
    r = requests.get(base_url+sectors_url)
    if r.status_code != requests.codes.ok:
        print("Could not open URL: ")+base_url+sectors_url
        
    sectors_page = BeautifulSoup(r.text,'lxml') #parses the html into a useful tree structure
    sectors = {}  #dict with sector names as keys.  Values are dicts with sector names as keys.
    template = re.compile(r'\d+conameu.html')
    for link in sectors_page.find_all('a'):
        if template.match(link['href']):
            sector_name = sectors[link.string]
            sector_name.replace('\n',' ',2)
            sectors[sector_name] = {'url':link['href'],'industries':{}}

    for k in sectors.keys():
        sector_url = sectors[k]['url']
        sector_response = requests.get(base_url+sector_url)
        sector_page = BeautifulSoup(sector_response.text,'lxml')
        if sector_response.status_code != requests.codes.ok:
            print("Could not open URL: "+sector_url)
        
        industries = {} #dict with industry name as key           
        template = re.compile(r'\d\d\d+conameu.html')
        for link in sector_page.find_all('a'):
            if template.match(link['href']):
                industry_string = link.string
                industry_string.replace('\n',' ',2)
                industries[industry_string] = {'url':link['href'], 'companies':{}}
        sectors[k]['industries'] = industries
#        break

    for ks in sectors.keys():
        industries = sectors[ks]['industries']
        for ki in industries.keys():
            companies = []
            url = industries[ki]['url']
            response = requests.get(base_url+url)
            page = BeautifulSoup(response.text,'lxml')
            if response.status_code != requests.codes.ok:
                print("Could not open URL: "+url)
            name_template = re.compile(r'biz.yahoo.com/p/\w/')
            ticker_template = re.compile(r'finance.yahoo.com/q\?s=(\w+)&')
            for link in page.find_all('a'):
                if name_template.search(link['href']):
                    ticker_link = link.find_next('a')
                    m = ticker_template.search(ticker_link['href'])
                    if m is None:
                        continue
                    symbol = m.group(1)
                    name = link.string
                    name.replace('\n',' ',2)
                    if symbol:
                        companies.append({'ticker':symbol.upper(),'name':name}) 
                    else:
                        print("Could not find symbol for link "+link['href'])
            industries[ki]['companies'] = companies
#            break
#        break

    return sectors


def build_sector_tables(engine, sectors):
    conn = engine.connect()
    sector_tables = ['sector','industry','company']

    for t in sector_tables:
        conn.execute('drop table if exists '+t)

    sector_list = sectors.keys()
    metadata = MetaData()
    sector_table = Table('sector', metadata,
                         Column('id', Integer, primary_key=True),
                         Column('sector', String(100)))

    industry_table = Table('industry', metadata,
                           Column('id', Integer, primary_key=True),
                           Column('sector', Enum(*sector_list)),
                           Column('industry', String(100)))

    company_table = Table('company', metadata,
                          Column('ticker', String(100)),  #this should be a primary key, but don't set it here to avoid errors
                          Column('name', String(100)),
                          Column('industry', String(100)))

    metadata.create_all(engine)

    ins = sector_table.insert()
    for s in sector_list:
        conn.execute(ins,sector=s) 

    ins_ind = industry_table.insert()
    ins_co = company_table.insert()
    for s in sectors.keys():
        industries = sectors[s]['industries']
        for i in industries.keys():
            companies = industries[i]['companies']
            conn.execute(ins_ind, sector=s, industry = i)
            for c in companies:
                conn.execute(ins_co, industry=i, ticker=c['ticker'], name=c['name'])
            
dbname = 'stocks'    
#sectors = {'foo':{'url':'zip','industries':{'indA':{'companies':[{'name':'coA','ticker':'COA'}]}}},'bar':{}}  #for dev only
sectors = get_industry_data()
print(sectors)
engine = DBEngine.create_engine(dbname)
build_sector_tables(engine, sectors)

        


    
