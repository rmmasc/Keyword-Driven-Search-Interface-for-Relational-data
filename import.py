# -*- coding: utf-8 -*-
"""
Created on Sat Apr 25 13:07:30 2020

@author: Royston, Rakshitha
"""

import requests
import mysql.connector as connector
import mysql
import pandas as pd
import os
import sys
import json
import requests
import time

base = "inf-551-be532.firebaseio.com"

def downData():
    url_down = f"https://{base}/.json"
    downResp = requests.get(url_down)
    downResp = downResp.json()
    print("Entire data downloaded from Firebase!")
    return downResp

def upNode(json):
    url_up = f"https://{base}/.json"
    print(url_up)
    upResp = requests.patch(url_up,json)
    if upResp.status_code == 200:
        print("New node uploaded in Firebase!")
    else:
        print("New node not uploaded in Firebase!Error:{}")


def exec_query(db_name,query):
    #base config to connect to database
    #-----!!!!!!CHANGE USER AND PASSWORD!!!!!-------
    config = {
      'user': 'inf551',
      'password': 'inf551',
      'host': '127.0.0.1',
      'database': db_name,
       'use_pure' : True,
    }
    
    #establish database connection 
    try:
        conn = connector.connect(**config)
        cursor = conn.cursor()
    except:
        print("Invalid Database name encountered.")
    
    #execute query using connection and cursor
    try:
        cursor.execute(query)
        rows = cursor.fetchall() #procure all rows, not just one (ex.generator)
        cursor.close()
        conn.close()
        return rows
    except connector.Error as e:
        cursor.close()
        conn.close()
        print(f"Error executing query: {e}")

def get_json(df,orient):
    if orient == 'records':
        return df.to_json(orient=orient,force_ascii=False)
    
'''
original
def clean_dataframe(df):
    new_cols = []
    for val in df.columns:
        new_cols.append(("".join((text if text.isalnum() else " ") for text in val)).strip())
    df.columns = new_cols
    columns = df.columns
    
    for col in columns:
        for val in df[col]:
            try:
                dtype = str(type(val))
                if not pd.isna(val):
                    break
            except:
                continue
        if 'str' in dtype:
            df[col] = df[col].str.replace("'","").str.replace('"','').str.strip()#.str.replace("#","").str.strip().str.replace("-","")
            df[col] = df[col].str.encode('ascii', 'ignore').str.decode('ascii')
    return df
'''

def clean_dataframe(df):
    '''
    cleans input data. Complexity: O(#tuples)
    '''
    new_cols = []
    #remove non alphanumeric characters
    for val in df.columns:
        new_cols.append(("".join((text if text.isalnum() else " ") for text in val)).strip())
    df.columns = new_cols
    columns = df.columns
    
    #check for datatype of column. If data type is non string, change whole column to string datatype
    for col in columns:
        for val in df[col]:
            try:
                dtype = str(type(val))
                if not pd.isna(val):
                    break
            except:
                continue
        if 'str' not in dtype:
            df[col] = df[col].astype(str)
        #try to enforce ascii encoding for better results during search algorithms.
        df[col] = df[col].str.replace("'","").str.replace('"','').str.strip()#.str.replace("#","").str.strip().str.replace("-","")
        df[col] = df[col].str.encode('ascii', 'ignore').str.decode('ascii')
    return df

class QUtils():
    '''
    a self made class that uses preset query templates to return commonly
    used queries
    '''
    def __init__(self):
        self.enabled = True
    
    def get_col_names(self,**kwargs):
        table,db = kwargs['table'],kwargs['db']
        return f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' and TABLE_SCHEMA = '{db}'"
    
    def get_table_names(self,**kwargs):
        db = kwargs['db']
        return f"SELECT table_name FROM information_schema.tables where table_schema = '{db}'"
    
    def get_key(self,**kwargs):
        table,db = kwargs['table'],kwargs['db']
        return f"SELECT key_column_usage.column_name FROM information_schema.key_column_usage WHERE table_schema = '{db}' AND constraint_name = 'PRIMARY' AND table_name = '{table}'"   

def invIndex(db, json_Dw):
    dbdict = {}
    ind = {}
    #print(json_Dw)
    prim2ind = {}
    requests.patch(f"https://{base}/prim2ind/{db}.json",'{}')
    for node, val_arr in json_Dw.items():
        prim2ind[node] = {}
        #requests.patch(f"https://{base}/prim2ind/{db}.json",json.dumps('{f"{node}":None}'))
        #print(node)
        #print(db)
        if db == "adventureworks":
            if node == "product":
                primCol = 'ProductID'
            elif node == "vendor":
                primCol = 'VendorID'
            elif node == "productvendor":
                primCol = ["ProductID","VendorID"]
        elif db == "chinook":
            if node == "album":
                primCol = 'AlbumId'
            elif node == "artist":
                primCol = 'ArtistId'
            elif node == "track":
                primCol = 'TrackId'
        elif db == "world":
            if node == "city":
                primCol = 'ID'
            elif node == "country":
                primCol = 'Code'
            elif node == "countrylanguage":
                primCol = ['CountryCode','Language']
        cnt = 0  
        #print(primCol)
        for val in val_arr:
            #print(val)
            for col,words in val.items():
                #print("primcol=",primCol, "value=",val[primCol[0]])
                if type(words) == str:
                    
                    s = "".join((char if char.isalnum() else " ") for char in words).lower().split()
                    for word in s:
                        #print(word)
                        details = {'COLUMN': col, 'INDEX': cnt, 'TABLE': node, 'PRIMARY KEY': None}
                        if type(primCol) == str:
                            #print("primcol=",primCol, "value=",val[primCol])
                            details['PRIMARY KEY'] = val[primCol]
                            prim2ind[node][details['PRIMARY KEY']] = details['INDEX']
                        else:
                            #print("primcol=",primCol, "value=",val[primCol[0]],"and",val[primCol[1]])
                            primArr = []
                            for prim in primCol:
                                primArr.append(val[prim])
                            details['PRIMARY KEY'] = primArr
                            prim2ind[node][f"/primArr[0]/primArr[1]"] = details['INDEX']

                        if word not in ind:
                            ind[word] = [details]
                        else:
                            ind[word].append(details)
                
                #patch to convert integers to string
                else:
                    words = str(words)
                    s = "".join((char if char.isalnum() else " ") for char in words).lower().split()
                    for word in s:
                        #print(word)
                        details = {'COLUMN': col, 'INDEX': cnt, 'TABLE': node, 'PRIMARY KEY': None}
                        if type(primCol) == list:
                            #print("primcol=",primCol, "value=",val[primCol[0]],"and",val[primCol[1]])
                            primArr = []
                            for prim in primCol:
                                primArr.append(val[prim])
                            primArr = [str(p) for p in primArr]
                            details['PRIMARY KEY'] = primArr
                            prim2ind[node][f"/primArr[0]/primArr[1]"] = details['INDEX']
                            #print("primcol=",primCol, "value=",val[primCol])
                        else:
                            details['PRIMARY KEY'] = str(val[primCol])
                            prim2ind[node][details['PRIMARY KEY']] = details['INDEX']
                            

                        if word not in ind:
                            ind[word] = [details]
                        else:
                            ind[word].append(details)
                #patch
                        
            cnt += 1
    #print(f"https://{base}/prim2ind/{db}.json")
    #print(requests.patch(f"https://{base}/prim2ind/{db}.json",json.dumps(prim2ind)))
    #print("Prim2Index created")
    dbdict[db] = ind
    print("Inverted Index dictionary created!")
    return dbdict
        
#invIndex(jsonDB)
def main():
    args = sys.argv[1:]
    helper = QUtils()
    dbs_list = [args[0]]
    dbs_node = args[1]
    starttime = time.time()
    base_url = f"https://{base}/.json"
    #print(requests.delete(base_url))
    requests.patch(f"https://{base}/columns.json",'{}')
    dbs2table = {'adventureworks':['product','vendor','productvendor'],'chinook':['track','album','artist'],'world':['city','country','countrylanguage']}
    for dbs in dbs_list:
        prim_url = f"https://{base}/{dbs_node}.json"
        a = requests.patch(prim_url,'{}')
        print(dbs,a)
        query = helper.get_table_names(db=dbs)
        #table_names = [tbl[0] for tbl in exec_query(dbs,query)]
        table_names = dbs2table[dbs]
        print(table_names)
        table2cols = {}
        for table_name in table_names:
            #connect to MySQL and get data
            query = helper.get_col_names(db=dbs,table=table_name)
            cols = [col[0] for col in exec_query(dbs,query)]
            table2cols[table_name] = cols
            #print(table_name,cols)
            query = f"SELECT * FROM {table_name}"
            data = exec_query(dbs,query)
            #clean data
            data = pd.DataFrame(data, columns = cols)
            df = clean_dataframe(data)
            #reorder data into JSON format
            json_data = get_json(df,'records')
            data = '{"' + table_name + '": ' + json_data + '}'
            #sec_url = f"https://inf-551-be532.firebaseio.com/{dbs}/{table_name}.json"
            #upload data
            resp = requests.patch(prim_url,data)
            sample = "Successful Upload" if '200' in str(resp)  else "Faulty upload"
            print(f"Response for {dbs}.{table_name} is {resp} " + sample)
        resp = requests.patch(f"https://{base}/columns/{dbs}.json",json.dumps(table2cols))
        sample = "Successful Upload" if '200' in str(resp)  else "Faulty upload"
        print(f"Response for {dbs} column upload is {resp} " + sample)
    
    jsonDB = downData()
    db1 = dbs_list[0] 
    for db, tab in jsonDB.items():
        #print(db)
        if db == db1:
            invInd = invIndex(db,tab)
    
    invIndjson = json.dumps(invInd)
    url_up = f"https://{base}/InvertedIndex.json"
    #print(url_up)
    upResp = requests.patch(url_up,invIndjson)
    if upResp.status_code == 200:
        print("New node uploaded in Firebase!")
    else:
        print("New node not uploaded in Firebase!Error:{}")
    endtime = time.time() - starttime 
    print(f"Time recorded: {endtime}")
    #invIndkey = '{"' + "InvertedIndex" + '": ' + invIndjson + '}'
    #upNode(invIndkey) 
            
if __name__ == "__main__":
    main()  