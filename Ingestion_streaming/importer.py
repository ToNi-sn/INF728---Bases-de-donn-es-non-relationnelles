#!/usr/bin/env python3

#----- IMPORT PACKAGES -----#
import logging
import random
import string
import requests
import re
import pandas as pd
import numpy as np
import time
import traceback
import sys
import logging
import time
import socket

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

filename_input = '/home/ubuntu/NoSQLProject/python_imports/'+sys.argv[1]

logging.basicConfig(filename=filename_input,level=logging.DEBUG)

ip = socket.gethostbyname(socket.gethostname())
# logging.debug('This is debug message')
# logging.info('This is information message')
# logging.warning('This is warning message')
# logging.error('This is warning message')

while True:
    KEYSPACE = "gdelt_prod"
    cluster = Cluster([ip])
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    
    #----- DEFINE FUNCTION TO -----#
    #       - DOWNLOAD .CSV FILES
    #       - CREATE TABLES FOR EACH REQUEST
    #       - INSERT TABLES IN CASSANDRA
    
    #----- DOWNLOAD .CSV FILES-----#
    def get_df(df_row):
        col2keep = {0: [0, 1, 2, 3, 12, 22, 31, 33, 34, 37, 45, 53, 60],
                    1: [0, 1, 5, 15],
                    2: [0, 1, 3, 4, 8, 10, 11, 12, 15]}
        colnames = {0: ["GlobalEventID",
                       "Day",
                       "Month",
                       "Year",
                       "Actor1_Type1Code",
                       "Actor2_Type1Code",
                       "NumMentions",
                       "NumArticles",
                       "AverageTone",
                       "Actor1_GeoCountryCode",
                       "Actor2_GeoCountryCode",
                       "Action_GeoCountryCode",  #pas utilisé 04/02
                       "SourceURL"],
                    1: ["GlobalEventID",
                        "EventTimeDate",
                        "MentionID",
                        "MentionDocTranslationalInfo"],
                    2:["GKGRecordID",
                       "Date", #V2_1_
                       "SourceCommonName", #V2_
                       "DocumentIdentifier", #V2
                       "Themes", #V2_Enhanced
                       "Locations", #V2_Enhanced
                       "Persons", #V1_ 
                       "Organization", #V2_Enhanced #pas utilisé 04/02
                       "Tone"]} #V2_
        names = ['events','mentions','gkg']
        errors = []
        df_list = {}
        url_list = [df_row['events'],df_row['mentions'],df_row['gkg']]
        
        for i, link in enumerate(url_list):
            df_list[names[i]]=pd.read_table(link,
                                            header=None,
                                            usecols=col2keep[i],
                                            names=colnames[i],
                                            encoding='ISO-8859-1')
        
        
        # GKG preprocessing
        gkg = df_list["gkg"].copy()
        gkg = gkg[gkg['Date'].notna()]
        gkg["Day"] = gkg["Date"].apply(lambda x: int(str(x)[:8]))
        gkg["Month"] = gkg["Date"].apply(lambda x: int(str(x)[:6]))
        gkg["Year"] = gkg["Date"].apply(lambda x: int(str(x)[:4]))
        gkg["Themes"] = gkg["Themes"].apply(lambda x: re.split(";|,", str(x))[:-1:2])
        gkg["Persons"] = gkg["Persons"].apply(lambda x: re.split(";", str(x)))
        gkg["Organization"] = gkg["Organization"].apply(lambda x: re.split(";|,", str(x))[:-1:2])
        gkg["Tone"] = gkg["Tone"].apply(lambda x: float(re.split(",", str(x))[0]))
        gkg["Locations"] = gkg["Locations"].apply(lambda x: str(x).replace("'", " "))
        gkg["Locations"] = gkg["Locations"].apply(lambda x: re.split(";|#", str(x))[1::9])
        
        gkg["Locations"] = gkg["Locations"].apply(lambda x: "{'no location(s) mentionned'}" if x==[] else "{'"+"', '".join(x)+"'}")
        gkg["Persons"] = gkg["Persons"].apply(lambda x: "{'no person(s) mentionned'}" if x==['nan'] else "{'"+"', '".join(x)+"'}")
        
        gkg["Themes"] =  gkg["Themes"].apply(lambda x: "{'"+"', '".join(x)+"'}")
        
        
        df_list["gkg"] = gkg 
        return df_list
        
        
    #----- CREATE TABLES FOR EACH REQUEST -----#
    def getTable1(df_list):
        table1 = df_list['events'][['GlobalEventID','Day','Action_GeoCountryCode']]\
            .merge(df_list['mentions'][['GlobalEventID','MentionID','MentionDocTranslationalInfo']],
                     on="GlobalEventID")
        table1 = table1[['GlobalEventID', 'Day', 'MentionID', 'Action_GeoCountryCode', 'MentionDocTranslationalInfo']]
        table1 =table1.fillna('NA')
        return table1
    
    def getTable2(df_list):
        table2 = df_list['events'][['GlobalEventID','Day','Month', 'Year','NumMentions','Action_GeoCountryCode']]
        return table2
    
    def getTable3(df_list):
        table3 = df_list['gkg'][["Day","Month",
                      "SourceCommonName",
                      "DocumentIdentifier",
                      "Themes",
                      "Locations",
                      "Persons",
                      "Tone"]]
        table3['Tone'] = table3['Tone'].apply(lambda x: round(x,6))             
        return table3
    
    def getTable4(df_list):
        table4 = df_list['events'][["SourceURL", "Day", "Month", "AverageTone", "Actor1_GeoCountryCode",
                                    "Actor2_GeoCountryCode"]]\
            .merge(df_list['gkg'][["DocumentIdentifier", "Themes"]],
                     left_on="SourceURL",
                     right_on="DocumentIdentifier")
        
        # Enlever l'une des colonnes ayant servis pour le merge.
        table4 = table4.drop(["DocumentIdentifier"], axis=1)
        
        return table4
    
    def makeTables(df_list):
        tables = {}
        tables['nb_articles_events']= getTable1(df_list)
        tables['countries_events']= getTable2(df_list)
        tables['data_source']= getTable3(df_list)
        tables['relationship']= getTable4(df_list)
        return tables
        
    
    #----- INSERT TABLES IN CASSANDRA -----#
    def SQL_INSERT_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET):
        sql_texts = []
        for index, row in SOURCE.iterrows():
            stat = 'INSERT INTO '+TARGET+' ('+ str(', '.join(SOURCE.columns))+ ') VALUES '+ str(tuple(row.values)).replace('\"{\'',"{'")
            stat = stat.replace("\'}\"","\'}")
           # stat = stat.replace("\'null\'"," null ")
            sql_texts.append( stat)
        return sql_texts
        
    def urls_to_import(year, month, day, hours, minutes, df_file_list):
        return df_file_list.filter(regex='^'+year+month+day+hours+minutes, axis=0)
        
    ########### Helpers    
        
    def fetch_and_mark_data(session, amount : int ):
        dates = []
        try:
            rows = session.execute('SELECT date FROM importstatus WHERE status = \'failed\' ALLOW FILTERING')
            for idx, user_row in enumerate(rows):
                dates.append(user_row.date)
                statement = f'UPDATE importstatus SET status = \'doing\'   WHERE date = {user_row.date} IF EXISTS'
                session.execute(statement )
                if idx  >amount-2:
                    break
                
        except Exception as e:
            print(e)
            print(statement)
        
        return dates
        
    def make_df(to_import):
        df = pd.DataFrame(to_import,columns =['date'])
        df['events'] = df['date'].apply(lambda x:'http://data.gdeltproject.org/gdeltv2/'+str(x)+'.export.CSV.zip' )
        df['mentions'] = df['date'].apply(lambda x:'http://data.gdeltproject.org/gdeltv2/'+str(x)+'.mentions.CSV.zip' )
        df['gkg'] = df['date'].apply(lambda x:'http://data.gdeltproject.org/gdeltv2/'+str(x)+'.gkg.csv.zip' )
        
        df = df.set_index('date')
    
        return df
        
        
        
    #########################################    
    ######################################### 
    ######################################### 
    ######################################### 
    df_urls_to_import = make_df(fetch_and_mark_data(session, 5))
    if len(df_urls_to_import) == 0: break
    doonnne =0
    errors = {}
    try:
        for index, urls in df_urls_to_import.iterrows():
            doonnne +=1
            try:
                start_time = time.perf_counter()
                df_list_events_mentions_gkg = get_df(urls)
                tables_for_requests = makeTables(df_list_events_mentions_gkg)
                status = {}
                all_good = True
                for table in tables_for_requests:
                    done = 0
                    SQL_test = SQL_INSERT_STATEMENT_FROM_DATAFRAME(tables_for_requests[table].fillna('null'), table)
            
                    for statement in SQL_test:
                        try:
                            session.execute(statement)
                            done+=1
                        except Exception as e:
                            errors[str(index) + '-' + str(done) + "-" +  table] = {"error" : e}
                            continue
                        
                    statement = f'UPDATE importstatus SET {table} = \'{done}/{len(SQL_test)}\'  WHERE date = {index} IF EXISTS'
                    if done != len(SQL_test) :
                        all_good = False
                    session.execute(statement)
                    
                    
                
                statement = f'UPDATE importstatus SET status = \'done\'  WHERE date = {index} IF EXISTS'
                if not all_good:
                    statement = f'UPDATE importstatus SET status = \'partial_done\'  WHERE date = {index} IF EXISTS'
                session.execute(statement)
                
                logging.info(f"{index} done in {time.perf_counter() - start_time}" )
            except Exception as e:
                statement = f'UPDATE importstatus SET status = \'failed\'  WHERE date = {index} IF EXISTS'
                session.execute(statement)
                logging.warning(f"{index} failed in  {time.perf_counter() - start_time}" )
                logging.warning(e)
                logging.warning(traceback.format_exc())
                
    except Exception as e:
        logging.warning(e)
        logging.warning(traceback.format_exc())
        logging.warning(f'untrusted statement {statement}')
        time.sleep(60*5)
        try:
            KEYSPACE = "gdelt_prod"
            cluster = Cluster([ip])
            session = cluster.connect()
            session.set_keyspace(KEYSPACE)
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            break
            
        
    
