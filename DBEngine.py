"""
DBEngine
Thin wrapper over pymsql.connections.Connection to minimize dependence on mysql-specific code.

usage:

import DBEngine
engine = DBEngine.create_engine(dbname)

Use "help(pymysql)" and "help(pandas.io.sql)" for more info.
"""

import pymysql
import sqlalchemy
_host = '73.17.18.177'
#_database = 'biopharma'
password_file = 'DBCredential.txt'

def create_engine(dbname):
    _user = ''
    _password = ''
    f = open(password_file,'r')
    for line in f:
        if line.startswith('#'):
            continue
        data = line.rstrip().split(':')
        if data[0] == 'user':
            _user = data[1]
        elif data[0] == 'password':
            _password = data[1]
        else:
            "Could not read credentials from " + password_file

    if _user == '':
        raise RunTimeError("user credential not found in " + password_file)
    if _password == '':
        raise RunTimeError("password credential not found in " + password_file)
    
    return sqlalchemy.create_engine("mysql+pymysql://" + _user + ":" + _password + "@" + _host + '/' + dbname)

# def connect_to_database(create=False):
#     engine = create_engine()
#     conn = engine.connect()

#     existing = conn.execute("show databases").fetchall()
#     existing = [x[0] for x in existing]  #extract from tuple 
#     if create and (dbname not in existing):
#         conn.execute('create database ' + dbname)
        
#     engine.execute('use ' + dbname)
#     conn.execute('use ' + dbname)
#     return conn
    
    

