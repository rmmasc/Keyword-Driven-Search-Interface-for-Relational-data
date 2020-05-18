# -*- coding: utf-8 -*-
"""
Created on Fri May  8 11:35:07 2020

@author: Rakshitha,Royston
"""


from flask import  Flask, request, render_template, session, redirect, url_for
import requests
import pandas as pd
import json
from search import *

app = Flask(__name__)

#redirect to the home screen
@app.route("/")
def home():
  return render_template("userInterface.html")

#on click of About, redirect it to the About Datasets page
@app.route("/About")
def About():
    return render_template("About.html")

#on click of Team Members tab, redirect it to the Team Members page
@app.route("/TeamMem")
def TeamMem():
    return render_template("TeamMem.html")

#on select of the databases dropdown 
@app.route('/db_filter',methods=['POST','GET'])
def db():
    text = request.form['dbDropDown']
    print("The DB selected is '" + text + "'")
    return render_template('userInterface.html',results=text)

#after submit, connecting to the backend and presenting the results in the tables sections
@app.route('/my_submit', methods=['POST','GET'])
def my_submit():
    db = request.form['dbDropDown']
    text = request.form['search']
    base = 'inf-551-be532.firebaseio.com'
    tokens = tokenize(text)
    
    db_dict = {'Adv':'adventureworks','Chi':'chinook','Wor':'world'}
    
    print(db,tokens,text)
    allRes, table_dict, rec = perform_search(tokens,db_dict[db],base)
    print("The DB is'" + db + "'")
    print("The text is '" + text + "'")
    
    print(table_dict)
    print(allRes)
    
    tb1name, tb2name, tb3name = table_dict.keys()

    tb1 = table_dict[tb1name]
    col_header1 = tb1.columns.values
    data1 = list(tb1.values.tolist())

    tb2 = table_dict[tb2name]
    col_header2 = tb2.columns.values
    data2 = list(tb2.values.tolist())

    tb3 = table_dict[tb3name]
    col_header3 = tb3.columns.values
    data3 = list(tb3.values.tolist())
    
    
    if db == 'Adv':
        lc1 = ['ProductID']
        lc2 = ['VendorID']
        lc3 = ['ProductID','VendorID']
    elif db == 'Chi':
        lc1 = ['ArtistId']
        lc2 = ['AlbumId', 'ArtistId']
        lc3 = ['AlbumId']
    elif db == 'Wor':
        lc1 = ['CountryCode']
        lc2 = ['Code']
        lc3 = ['CountryCode']
    
    
    
    return render_template('userInterface.html', dbname = db_dict[db],tb1name=tb1name, tb2name=tb2name,
                          tb3name=tb3name, col_header = allRes.columns.values, col_header1 = col_header1,col_header2 = col_header2,
                          col_header3 = col_header3,
                          data = list(allRes.values.tolist()),data1 = data1, data2 = data2, data3= data3,
                          lc1 = list(lc1), lc2 = list(lc2), lc3 = list(lc3),
                          recTime = rec, zip=zip)
    
#to navigate to the foreign key relationships
@app.route('/leadsto/<string:rowname>/<string:colname>/<string:tbname>/<string:dname>', methods=['POST','GET'])    
def leadsto(rowname, colname, tbname, dname):
    

    if request.method != "POST":
    
        print(rowname, colname, tbname, dname)
        table_dict, rec = get_foreign_links(db=dname, table=tbname, attr=colname, text=rowname)
        
        tb1name, tb2name, tb3name = table_dict.keys()
        #print(tb1name, tb2name, tb3name)
        tb1 = table_dict[tb1name]
        col_header1 = tb1.columns.values
        data1 = list(tb1.values.tolist())

        tb2 = table_dict[tb2name]
        col_header2 = tb2.columns.values
        data2 = list(tb2.values.tolist())

        tb3 = table_dict[tb3name]
        col_header3 = tb3.columns.values
        data3 = list(tb3.values.tolist())
        
        
        if dname == 'adventureworks':
            lc1 = ['ProductID']
            lc2 = ['VendorID']
            lc3 = ['ProductID','VendorID']
        elif dname == 'chinook':
            lc1 = ['ArtistId']
            lc2 = ['AlbumId', 'ArtistId']
            lc3 = ['AlbumId']
        elif dname == 'world':
            lc1 = ['CountryCode']
            lc2 = ['Code']
            lc3 = ['CountryCode']
        
        return render_template('userInterface.html', dbname = dname,tb1name=tb1name, tb2name=tb2name,
                              tb3name=tb3name, col_header1 = col_header1,col_header2 = col_header2,
                              col_header3 = col_header3,
                              data1 = data1, data2 = data2, data3= data3,
                              lc1 = list(lc1), lc2 = list(lc2), lc3 = list(lc3),
                              recTime = rec, zip=zip)
    else:
        db = request.form['dbDropDown']
        text = request.form['search']
        print(text, db)
        
        base = 'inf-551-be532.firebaseio.com'
        tokens = tokenize(text)
        
        db_dict = {'Adv':'adventureworks','Chi':'chinook','Wor':'world'}
        
        print(db,tokens,text)
        allRes, table_dict, rec = perform_search(tokens,db_dict[db],base)
        print("The DB is'" + db + "'")
        print("The text is '" + text + "'")
        
        print(table_dict)
        
        print(allRes)
        
        tb1name, tb2name, tb3name = table_dict.keys()

        tb1 = table_dict[tb1name]
        col_header1 = tb1.columns.values
        data1 = list(tb1.values.tolist())

        tb2 = table_dict[tb2name]
        col_header2 = tb2.columns.values
        data2 = list(tb2.values.tolist())

        tb3 = table_dict[tb3name]
        col_header3 = tb3.columns.values
        data3 = list(tb3.values.tolist())
        
        
        if db == 'Adv':
            lc1 = ['ProductID']
            lc2 = ['VendorID']
            lc3 = ['ProductID','VendorID']
        elif db == 'Chi':
            lc1 = ['ArtistId']
            lc2 = ['AlbumId', 'ArtistId']
            lc3 = ['AlbumId']
        elif db == 'Wor':
            lc1 = ['CountryCode']
            lc2 = ['Code']
            lc3 = ['CountryCode']
        

        return render_template('userInterface.html', dbname = db_dict[db],tb1name=tb1name, tb2name=tb2name,
                          tb3name=tb3name, col_header = allRes.columns.values, col_header1 = col_header1,col_header2 = col_header2,
                          col_header3 = col_header3,
                          data = list(allRes.values.tolist()),data1 = data1, data2 = data2, data3= data3,
                          lc1 = list(lc1), lc2 = list(lc2), lc3 = list(lc3),
                          recTime = rec, zip=zip)
            
        
if __name__ == "__main__":
  app.run(debug=True)