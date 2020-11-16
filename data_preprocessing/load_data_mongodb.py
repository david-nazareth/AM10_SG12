# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 14:13:23 2020
@author: brend
"""

import os
import gzip
import numpy as np
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import pymongo


flights_data_directory = r"data\flights_data_opensky"
aircraft_dbs_directory = r"data\aircraftDatabases"

mongoDB_Db = "OpenSky_Covid19"
mongoDB_Col = "Master"

columns_drop_airdb = ["registration", "manufacturericao", "typecode", "serialnumber", "linenumber", "icaoaircrafttype",  
                      "operatoriata", "testreg", "reguntil", "status", "firstflightdate", "seatconfiguration", "engines", "modes", 
                      "adsb", "acars", "notes", "categoryDescription"]

extract_files = False



#Create MongoDB connection
def createMongo(mongoDB_Db, mongoDB_Col):
    import pymongo
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[mongoDB_Db]
    mycol = mydb[mongoDB_Col]
    return mycol


# To extract .gz file to .csv
def extract_data_files(data_directory):
    def extract_gzip(gzip_file):
        with gzip.GzipFile(gzip_file, 'rb') as f:
            data = f.read()
        with open(gzip_file[:-3], 'wb') as f:
            f.write(data)
    
    files = [os.path.join(data_directory, f) for f in os.listdir(data_directory) if ".gz" in f]
    [extract_gzip(file) for file in files]
    
    
def get_most_recent_airdb(airDbs, flights_date):
    this_table = {k: abs((datetime.strptime(k, "%Y%m%d") - datetime.strptime(flights_date, "%Y%m%d")).days) for k in airDbs.keys()}
    recent_airdb = [k for k in this_table.keys() if this_table[k] == min(this_table.values())][0]
    return recent_airdb
    
    
def get_aircraftDbs(aircraft_dbs_directory):
    airDbs = {}
    files = [os.path.join(aircraft_dbs_directory, file) for file in os.listdir(aircraft_dbs_directory) if ".csv" in file]
    for file in files:
        df = pd.read_csv(file, low_memory = False)
        df.drop(columns = [c for c in columns_drop_airdb if c in df.columns], inplace = True)
        df.dropna(subset = ["icao24"], inplace = True)
        df["operator"] = df.apply(lambda x: x["operatorcallsign"] if x["operator"] is np.NaN else x["operator"], axis = 1)
        df.drop(columns = ["operatorcallsign"], inplace = True)
        airDbs[file.split(" - ")[-1].replace(".csv", "").replace("-", "")] = df
    return airDbs
    

def insert_in_mongodb(csv_file, airDbs, mycol):
    df = pd.read_csv(csv_file, low_memory = False)
    df.drop(columns = ["number"], inplace = True)
    df.dropna(subset = ["latitude_1", "latitude_2", "longitude_1", "longitude_2"], inplace = True)
    df.drop_duplicates(subset = ["icao24", "firstseen"], inplace = True)
    df.drop_duplicates(subset = ["icao24", "lastseen"], inplace = True)
    recent_airdb = get_most_recent_airdb(airDbs, csv_file.split("_")[-1].replace(".csv", ""))
    df["firstseen"] = df["firstseen"].apply(lambda x: datetime.fromisoformat(x))
    df["lastseen"] = df["lastseen"].apply(lambda x: datetime.fromisoformat(x))
    df["day"] = df["day"].apply(lambda x: datetime.fromisoformat(x))
    df = pd.merge(df, airDbs[recent_airdb], how = "left", on = "icao24")
    df_cols = ["icao24"] + [c for c in ["callsign", "registration", "typecode", "origin", "destination", "day", "firstseen", "lastseen", 
                                        "manufacturername", "model", "operator", "operatoricao", "owner", "registered", "built", 
                                        "latitude_1", "longitude_1", "altitude_1", "latitude_2", "longitude_2", "altitude_2"] if c in df.columns]
    df = df[df_cols]
    df_dict = df.to_dict(orient = "records")
    for this_dict in df_dict:
        this_dict["coordinates_1"] = [this_dict.pop("longitude_1"), this_dict.pop("latitude_1")]
        this_dict["coordinates_2"] = [this_dict.pop("longitude_2"), this_dict.pop("latitude_2")]
    response = bulk_write(mycol, df_dict)
    status_name = csv_file.split("\\")[-1].replace(".csv", "")
    status_nb = len(df_dict)
    return status_name, status_nb, response


def bulk_write(mycol, data):
    bulk = pymongo.bulk.BulkOperationBuilder(mycol, ordered = False)
    for item in data:
        bulk.insert(item)
    try:
        response = bulk.execute()
    except pymongo.errors.BulkWriteError:
        pass
    return response


def prepare_db(mycol):
    mycol.create_index([("icao24", pymongo.ASCENDING), ("firstseen", pymongo.DESCENDING)], name = "icao24_firstseen", unique = True)
    mycol.create_index([("icao24", pymongo.ASCENDING), ("lastseen", pymongo.DESCENDING)], name = "icao24_lastseen", unique = True)
    mycol.create_index([("coordinates_1", pymongo.GEO2D)], name = "coordinates_1", unique = False)
    mycol.create_index([("coordinates_2", pymongo.GEO2D)], name = "coordinates_2", unique = False)
    
    
def main():
    print("Starting")
    if extract_files:
        print("Extracting files...")
        extract_data_files(flights_data_directory)
    else:
        print("Skipping files extraction")
    print("Loading aircraftDbs...")
    airDbs = get_aircraftDbs(aircraft_dbs_directory)
    print("Connecting to MongoDb...")
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    print("Preparing db...")
    prepare_db(mycol)
    csv_files = [os.path.join(flights_data_directory, f) for f in os.listdir(flights_data_directory) if ".csv" in f]
    print("Found %r CSV files" % len(csv_files))
    status_main = []
    with tqdm(total = len(csv_files)) as pbar:
        for csv_file in csv_files:
            status_name, status_nb, response = insert_in_mongodb(csv_file, airDbs, mycol)
            status_main.append({"File": status_name, "# Flights": status_nb})
            pbar.update(1)
    print("Complete")
    print(pd.DataFrame(status_main)) 
