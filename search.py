import requests
import pandas as pd
import os
import csv
import pandas as pd
import requests
import json
import time
from collections import defaultdict
base = 'inf-551-be532.firebaseio.com'


class ResultObject():
    #store attributes of result for transparent code
    def __init__(self,loc,tupl,pkey=None,text=None):
        self.text = text
        self.score = 0
        self.loc = loc
        self.pkey = pkey if type(pkey) != list else tuple(pkey)
        self.table,self.ind,self.col = loc['TABLE'], loc['INDEX'], loc['COLUMN']
        self.links = []
        self.tupl = tupl
        self.unity = 0
        
        
def tokenize(query):
    a = query.lower()
    b = "".join((i if i.isalnum() else " ") for i in a).strip().split()
    b = a.split()
    return b
    
def perform_search(tokens,db,base):
    #stores token v/s location in database
    query2loc = {}
    starttime = time.time()
    for token in tokens:
        #for each word in search query, get the locations associated
        search_url = f"https://{base}/InvertedIndex/{db}/{token}.json"
        resp = requests.get(search_url).json()
        #store location of token information in query to location dict
        query2loc[token] = resp 

    search_results = []
    main_query = " ".join(tokens)

    #for every token, store the content of where the token is pointing to and the score 
    for query,loclist in query2loc.items():
        if loclist:
            for loc in loclist:
                table,col,index,pkey = loc['TABLE'],loc['COLUMN'], loc['INDEX'],loc['PRIMARY KEY']
                request_url = f"https://{base}/{db}/{table}/{index}.json"
                tupl = requests.get(request_url).json()
                obj = ResultObject(loc,tupl)
                obj.text = tupl[col].lower() if type(tupl[col])==str else str(tupl[col]).lower()
                obj.tupl = tupl
                obj.pkey = pkey  if type(pkey) != list else tuple(pkey)
                #add to unity attribute if the query is contained in the text.
                if main_query in obj.text:
                    obj.unity+=1
                #add to score based on word overlap
                for word in obj.text.split():
                    for q in tokens:
                        if word == q:
                            obj.score += 1
                search_results.append(obj)


    table2result = {}
    db2table = {'adventureworks':['product','vendor','productvendor'],
                'world':['city','country','countrylanguage'],
                'chinook':['artist','album','track']}
    nodelst = db2table[db]
    for node in nodelst:
        table2result[node] = sorted([r for r in search_results if r.table==node],key=lambda x:(x.unity,x.score),reverse=True)

    collector = {}
    
    #separate each result by table
    filtered = {nodelst[0]:[],nodelst[1]:[],nodelst[2]:[]}

    for k,v in table2result.items():
        for item in v:
            idx = (item.table,item.ind,item.col)
            if idx in collector: 
                pass
            else:
                collector[idx] = None
                filtered[k].append(item)
    
    #merge duplicate results
    merged = {}
    for k,v in filtered.items():
        merged[k] = {}
        for item in v:
            if item.pkey not in merged[k]:
                merged[k][item.pkey] = [item]
            else:
                merged[k][item.pkey].append(item)

    #compute final results sorted by unity score (all keywords in one attribute )
    #and then sorted by score (+! for every matching keyword)
    final_result = {}
    for k,subdict in merged.items():
        final_result[k] = []
        for pkey, items in subdict.items():
            main = items[0]
            for i in range(1,len(items)):
                item = items[i]
                main.links.append(item)
                main.score+=item.score
            final_result[k].append(main)
    final_result[k] = sorted(final_result[k],key=lambda x:(x.unity,x.score),reverse=True)
    
    #for results ranked together irrespective of table
    combi = []
    for table, objects in final_result.items():
        combi.extend((table,ob) for ob in objects)
    combi = sorted(combi,key=lambda x:(x[1].unity,x[1].score),reverse=True)
    
    #adjust table since columns vary for each table
    rows = []
    maxlen = -1
    for element in combi:
        table, ob = element
        sample = [table] + list(ob.tupl.values())
        rows.append(sample)
        if maxlen < len(sample):
            maxlen = len(sample)
    
    for row in rows:
        if len(row) < maxlen:
            row.extend([None]*(maxlen-len(row)))

            
    all_results = pd.DataFrame(rows,columns= None)

    q = " ".join(i for i in tokens) ;
    table2df = {}
    switch = False
    for table, result in final_result.items():
        request_url = f"https://{base}/columns/{db}/{table}.json"
        column_names = requests.get(request_url).json()
        print(table,column_names,request_url)
        table_dict = defaultdict(list)
        for res in result:
            switch = True
            tupl = res.tupl
            for col in column_names:
                if col in tupl.keys():
                    table_dict[col].append(tupl[col])
                else:
                    table_dict[col].append(None)   
        table2df[table] = pd.DataFrame.from_dict(table_dict) if switch else pd.DataFrame([[None]*len(column_names)],columns=column_names)
        switch = False
    endtime = time.time() - starttime
    return all_results, table2df, endtime

def get_foreign_links(db,table,attr,text):
    
    #relations obtained from MySQL server
    fkey_relations = {'adventureworks':{'product/ProductID':{'product':'ProductID','vendor':'VendorID','productvendor':'ProductID'},
                   'vendor/VendorID':{'product':'VendorID','vendor':'VendorID','productvendor':'VendorID'},
                   'productvendor/VendorID':{'product':'VendorID','vendor':'VendorID','productvendor':'VendorID'},
                   'productvendor/ProductID':{'product':'ProductID','vendor':'ProductID','productvendor':'ProductID'}},
                  'world':{'country/Code':{'city':'CountryCode','country':'Code','countrylanguage':'CountryCode'},
                          'city/CountryCode':{'city':'CountryCode','country':'Code','countrylanguage':'CountryCode'},
                          'countrylanguage/CountryCode':{'city':'CountryCode','country':'Code','countrylanguage':'CountryCode'}},
                  'chinook':{'artist/ArtistId':{'artist':'ArtistId','album':'ArtistId','track':'ArtistId'},
                     'track/AlbumId':{'artist':'AlbumId','album':'AlbumId','track':'AlbumId'},
                     'album/AlbumId':{'artist':'AlbumId','album':'AlbumId','track':'AlbumId'},
                     'album/ArtistId':{'artist':'ArtistId','album':'ArtistId','track':'ArtistId'}}}
    
    starttime = time.time()
    #get foreign key for each table
    #switch to simulate empty dataframe if no data is found for that trigger
    switch = False
    table2res = {}
    trigger = f"{table}/{attr}"
    #trigger dictionary realized what has to be queried based on on click event
    fkey_trigger = fkey_relations[db][trigger]
    for table, fkey in fkey_trigger.items():
        if type(text) == str:
            query = f'https://{base}/{db}/{table}.json?orderBy="{fkey}"&equalTo="{text}"'
        else:
            query = f'https://{base}/{db}/{table}.json?orderBy="{fkey}"&equalTo={text}'
        #use firebase index to get the results for the respective trigger
        resps = requests.get(query).json().values()
        request_url = f"https://{base}/columns/{db}/{table}.json"
        columns = requests.get(request_url).json()
        tdict = defaultdict(list)
        #compile query results into dataframes
        for resp in resps:
            switch = True
            for col in columns:
                if col in resp:
                    tdict[col].append(resp[col])
                else:
                    tdict[col].append(None)
        table2res[table] = pd.DataFrame.from_dict(tdict) if switch else pd.DataFrame([[None]*len(columns)],columns=columns) 
        switch = False
    rec = time.time() - starttime 
    return table2res,rec

'''
#functions for index search.. tested but replaced by functions above.

def get_primCol(db,node):
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
    return primCol
    
def perform_index_search(tokens,db,base):    
    #stores token v/s location in database
    query2loc = {}
    starttime = time.time()
    for token in tokens:
        #for each word in search query, get the locations associated
        search_url = f"https://{base}/InvertedIndex/{db}/{token}.json"
        resp = requests.get(search_url).json()
        #store location of token information in query to location dict
        query2loc[token] = resp 

    search_results = []
    main_query = " ".join(tokens)

    #for every token, store the content of where the token is pointing to and the score 
    for query,loclist in query2loc.items():
        if loclist:
            for loc in loclist:
                table,col,index,pkey = loc['TABLE'],loc['COLUMN'], loc['INDEX'],loc['PRIMARY KEY']
                pkey_attr = get_primCol(db,table)
                if type(pkey) != list:
                    request_url = f'https://{base}/{db}/{table}.json?orderBy="{pkey_attr}"&equalTo="{pkey}"'
                    tupl = list(requests.get(request_url).json().values())[0]
                else:
                    pkey1,pkeyattr1 = pkey[0],pkey_attr[0]
                    pkey2, pkeyattr2 = pkey[1], pkey_attr[1]
                    request_url = f'https://{base}/{db}/{table}.json?orderBy="{pkeyattr1}"&equalTo="{pkey1}"'
                    tupl_props = requests.get(request_url).json().values()
                    for prop in tupl_props:
                        if prop[pkeyattr2] == pkey2:
                            tupl = prop
                            break
                obj = ResultObject(loc,tupl)
                obj.text = tupl[col].lower() if type(tupl[col])==str else str(tupl[col]).lower()
                obj.tupl = tupl
                obj.pkey = pkey  if type(pkey) != list else tuple(pkey)
                #add to unity attribute if the query is contained in the text.
                if main_query in obj.text:
                    obj.unity+=1
                #add to score based on word overlap
                for word in obj.text.split():
                    for q in tokens:
                        if word == q:
                            obj.score += 1
                search_results.append(obj)


    table2result = {}
    db2table = {'adventureworks':['product','vendor','productvendor'],
                'world':['city','country','countrylanguage'],
                'chinook':['album','artist','track']}
    nodelst = db2table[db]
    for node in nodelst:
        table2result[node] = sorted([r for r in search_results if r.table==node],key=lambda x:(x.unity,x.score),reverse=True)

    collector = {}

    filtered = {nodelst[0]:[],nodelst[1]:[],nodelst[2]:[]}

    for k,v in table2result.items():
        for item in v:
            idx = (item.table,item.ind,item.col)
            if idx in collector: 
                pass
            else:
                collector[idx] = None
                filtered[k].append(item)

    merged = {}
    for k,v in filtered.items():
        merged[k] = {}
        for item in v:
            if item.pkey not in merged[k]:
                merged[k][item.pkey] = [item]
            else:
                merged[k][item.pkey].append(item)

    final_result = {}
    for k,subdict in merged.items():
        final_result[k] = []
        for pkey, items in subdict.items():
            main = items[0]
            for i in range(1,len(items)):
                item = items[i]
                main.links.append(item)
                main.score+=item.score
            final_result[k].append(main)
        final_result[k] = sorted(final_result[k],key=lambda x:(x.unity,x.score),reverse=True)

    q = " ".join(i for i in tokens) ;
    table2df = {}
    switch = False
    for table, result in final_result.items():
        request_url = f"https://{base}/columns/{db}/{table}.json"
        column_names = requests.get(request_url).json()
        print(table,column_names,request_url)
        table_dict = defaultdict(list)
        for res in result:
            switch = True
            tupl = res.tupl
            for col in column_names:
                if col in tupl.keys():
                    table_dict[col].append(tupl[col])
                else:
                    table_dict[col].append('None')   
        table2df[table] = pd.DataFrame.from_dict(table_dict) if switch else pd.DataFrame([[None]*len(column_names)],columns=column_names)
        switch = False
    endtime = time.time() - starttime
    print(endtime)
    return table2df, endtime
'''




