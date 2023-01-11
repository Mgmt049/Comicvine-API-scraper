# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 22:40:38 2022

@author: kevinfaterkowski
"""
#https://comicvine.gamespot.com/api/

import pandas as pd
import requests 
import json 
import sys
import os #for file path testing
import datetime
import shutil as sh
#from XlsxWriter import FileCreateError

def load_previous(dir_output):
    
    print("timestamp pulled in load_previous:", datetime.datetime.now())
    
    dir_output = dir_output+'Comicvine.xlsx'
    
    #setup the error log:
    with open("API_error_log.txt", mode="a") as err_file:
        try:
            if os.path.isfile(dir_output):
                
                #you must specify index_col=0 to prevent new indexes from being created
                full_data = pd.read_excel(dir_output, index_col=0)
                print("dataframe shape in load_previous: ", full_data.shape)
                
                #return a dataframe    
                return full_data
            else: 
                return pd.DataFrame() #return an empty dataframe
        
        except FileNotFoundError as e:
            print("this the FNF error", e)
            err_file.write("this the FNF error %s "%(datetime.datetime.now()) )
            sys.exit() #terminate the whole program
        except IOError as io:
            print("this the IO error: ", io)
            #err_file.write("this the IO error", datetime.datetime.now())
            err_file.write("this the IO error %s "%(datetime.datetime.now()) )
            sys.exit() #terminate the whole program
        #except FileCreateError as fce:
        #    print("this the IO error: ", fce)
        #    #err_file.write("this the FileCreateError error", datetime.datetime.now())
        #    err_file.write("this the FileCreateError error %s"%(datetime.datetime.now()) )
        #    sys.exit() #terminate the whole program


def build_query_string(base_endpt, offset):
    
    CV_API_KEY = "f4c0a0d5001a93f785b68a8be6ef86f9831d4b5b" #do not use quotes around the key!
    CV_resource = "characters"
    CV_query_string = "/?api_key="
    CV_filter_string = ""
    
    #https://comicvine.gamespot.com/forums/api-developers-2334/paging-through-results-page-or-offset-1450438/
    #The end of the "characters" resource list is around 149150
    CV_sort_offset_string = "&sort=name: asc&offset=%s"%(offset)
    
    resp_format = "&format=json"
    return base_endpt + CV_resource + CV_query_string + CV_API_KEY + CV_filter_string + CV_sort_offset_string + resp_format

def normalize_df(json_CV):
    
    #grab the current date for timestamping
    formatted_date = datetime.datetime.now()
    formatted_date = formatted_date.strftime('%M-%D-%Y')
    print("timestamp pulled in normalize_df() %s"%(datetime.datetime.now()))
    
    json_CV = pd.json_normalize(json_CV, record_path =['results'],meta=['error', 'limit', 'offset'])
    #append the timestamp column onto the dataframe
    json_CV['TS_pulled'] = datetime.datetime.now()
    return json_CV

def calc_offset(df):
    #The end of the "characters" resource list is around 149150
    #use len() to return number of rows
    return ( len(df) + 1 )

def make_request(full_endpt, headers, offset):
    resp_CV = requests.get(full_endpt, headers = headers)
    
    #a response of 200 is OK
    print(resp_CV)
        
    #for value in resp_CV.headers:
    #    #use value as an index to perform a lookup
    #    print(value, ":", resp_CV.headers[value])
    
    if resp_CV.status_code == 200: #test for succesful response
    #NOTE: you must use the .json() or json.dumps() methods to ensure the object is serializable
        obj_json = json.dumps(resp_CV.json(), indent=4)
        
        #print("type of json object?: ", resp_CV.json().length)
        
        if not resp_CV:
            print("no more results from API call.")
            sys.exit()
        
        with open("temp_json.json", "w") as file_json:
            file_json.write(obj_json)
        #You use json.loads to convert a JSON string into Python objects needed  to read nested columns
        with open("temp_json.json",'r') as file_json:
            json_CV = json.loads(file_json.read())
        
        #TEST
        #json_CV['TS_pulled']=datetime.datetime.now()
        #with open('json_w_TS','w') as j_file:
        #    j_file.write(json_CV)
        #TEST
        
        return json_CV #return a json object
            
    else: 
        print("bad response, put in a try-catch")

def combine_dfs(dfs):
    #concat must be passed an "iterable"/"array" of Dataframe objects, I believe ignore_index is
    #necessary for re-numbering the index
    return pd.concat(dfs, axis=0, ignore_index=True)

def write_results(df_full_data, path_output):
    #setup the error log:
    with open("API_error_log.txt", mode="a") as err_file:

        path_output = path_output + "Comicvine.xlsx"        

        try:
            
            #quickly create a backup file
            sh.copy2(path_output, 'C:\\Users\\00616891\\Downloads\\CV_API_output\\Comicvine_bak.xlsx')
            
            #df_full_data.to_excel(path_output)
            

            
            #Excel threw a hard limit on 65K+ URLS error, so i had to use Excelwriter() and ingore URLs instead of .toExcel()
            #https://pandas.pydata.org/docs/reference/api/pandas.ExcelWriter.html
            #https://stackoverflow.com/questions/55280131/no-module-named-xlsxwriter-error-while-writing-pandas-df-to-excel/55280686
            with pd.ExcelWriter(path_output, engine='xlsxwriter', options={'strings_to_urls': False}) as writer:
                df_full_data.to_excel(writer)
            
            print("timestamp pulled in write_results() %s"%(datetime.datetime.now()))

        except FileNotFoundError as e:
            print("this the FNF error", e)
            err_file.write("this the FNF error %s "%(datetime.datetime.now()) )
            sys.exit() #terminate the whole program
        except IOError as io:
            print("this the IO error: ", io)
            err_file.write("this the IO error %s "%(datetime.datetime.now()) )
            sys.exit() #terminate the whole program
        #except FileCreateError as fce:
        #    print("this the IO error: ", fce)
        #    #err_file.write("this the FileCreateError error", datetime.datetime.now())
        #    err_file.write("this the FileCreateError error %s"%(datetime.datetime.now()) )
        #    sys.exit() #terminate the whole program


def main():


    base_endpt = "http://comicvine.gamespot.com/api/"
    #you must include this headers parameters because the comicvine API requires a "unique user agent" - cannot be null
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}

    
    path_output='C:\\Users\\00616891\\Downloads\\CV_API_output\\'
    
    #return a dataFrame
    df_full_data = load_previous(path_output)
    
    #retrieve offset for query string as an integer
    offset = calc_offset(df_full_data)   
    
    #pass an integer and retrieve a full http query string
    full_endpt = build_query_string(base_endpt, offset)
    
    #print(full_endpt)
    
    #request JSON
    json_CV = make_request(full_endpt, headers, offset)
    
    # Normalizing data - creates a dataFrame
    df_CV_norm = normalize_df(json_CV)
    
    df_full_data = combine_dfs([df_full_data,df_CV_norm]) #pass a list of dataframes
        
    print("df_full_data in main(): ", df_full_data.shape)
    
    #write combined results to file
    write_results(df_full_data, path_output)
    
    #df_CV_norm = df_CV_norm.dropna()  #delete null values, BE CAREFUL WITH THIS AND NORMALIZATION
    #the following uses the loc operator
    #display(df_CV_norm.loc[:, "results"])
    #the following uses the indexing operator - KEEP IN MIND THAT YOU HAVE TO NEST BRACKETS FOR A 2D DATAFRAME
    #display(df_CV_norm[["results"]])
     
        
if __name__ == "__main__":
    main()

