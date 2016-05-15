"""
Script for uploading the FDA events table to the SQL database.

TO DO: dates get uploaded as strings.  Process dataframe so dates become dates.  
Or alter table after upload.
"""

import sys
    
import pandas as pd
#from pandas.io.sql import SQLDatabase as pdsql

import DBEngine

source_file = '~/work/BioPharmaEvents/drug_event_db_v03.csv'
events_table = 'FDAEvents'

def load_source_file():
    try:
        df = pd.read_csv(source_file)
    except UnicodeDecodeError:
        return pd.read_csv(source_file,encoding='latin-1')
    except:
        print("Could not read source file")
        sys.exit(1)
        
    df.fillna('') #avoid problems caused by NULLs
    df.drop_duplicates(subset=df.columns.delete(0))

def build_events_table(df, engine):
    df_old = None ;
    if ( engine.has_table(events_table)):
       #preserve existing additional info in sql table
       df_old = read_events_table(engine)
       extra_columns = set()
       if len(df_old) > 0:
           extra_columns = set(df_old.columns).difference(df.columns)
       if len(extra_columns) > 0:
           df_old.fillna('')
           merge_columns = list(df.columns.delete(0)) # ignore Index when merging
           if 'ticker' in merge_columns:
               merge_columns.remove('ticker')
           df = pd.merge(df, df_old, on=merge_columns)
       else:
           df_old = None   #Nothing to preserve

    if 'ticker' not in df.columns:
        df['ticker'] = ['' for i in range(0,len(df))]
    # For each row, if ticker is missing, try to match companyName with known company names and fill ticker value if matched.
    tried = {} #dictionary of names tried
    ans = ''  #ticker input from terminal, or 'quit'
    for i in range(0,len(df)):
        if ans == 'quit':
            break
        ticker = df['ticker'][i]
        name = df['companyName'][i]
        print("Processing " + name)
        if ticker is not None and ticker != '':
            tried[name] = ticker
            continue
        if tried.get(name) is None:
            print("Trying "+name)
            tried[name] = ''
            res = engine.execute('select name, ticker, match(name) against ("' + name + '" in boolean mode) as score from company as c left join industry as i using (industry) where i.sector="Healthcare" having score>5;')
            results = res.fetchall()
            print(results)
            if results:
                results.sort(key = lambda x: x[2], reverse=True) #sort by match score
                for row in results:
                    print("Matched " + name + "with " + str(row))
                    print("Accept this match? (y/n)")
                    ans = sys.stdin.readline().rstrip()
                    if ans == 'y':
                        tried[name] = ticker = row[1]
                        break
                    elif ans == 'n' or ans == '':
                        continue
                    elif ans == 'quit':
                        break
                    else: # user supplied ticker string
                        tried[name] = ticker = ans
                        break
                else:
                    print("Could not match " + name)
        else:
            ticker = tried[name]
        if ticker:
            df['ticker'][i] = ticker ;
           
    df.to_sql(events_table, engine, if_exists='replace', index=False)
       

def read_events_table(engine):
    return pd.read_sql_table(events_table, engine)

# Body of program

print("This program has already been run.  Please backup the mysql FDAEvents table before running it again.")
print("Otherwise you may wipe out ticker info that has been entered by hand.");
print("Are you sure you want to run the program now? (yes/no)")
rerun = sys.stdin.readline().rstrip()
if rerun != 'yes':
    sys.exit(0)

df = load_source_file()               
engine = DBEngine.create_engine('stocks')  # get DB engine for 'stocks' database
build_events_table(df, engine)
df2 = read_events_table(engine)  # for verification

#print(df2)

