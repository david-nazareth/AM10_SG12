# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 13:55:33 2020

@author: brend
"""

import sys
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
from tqdm import tqdm
import pymongo
import random


mongoDB_Db = "OpenSky_Covid19"
mongoDB_Col = "Master"

airports_info_file = "data/airport-codes_csv.csv"
covid_data = "data/covid_data.xlsx"


#List of criteria with column name to use to extract airlines
criteria_list = {"US AirForce": {"operator": "United States Air Force"}, 
                 "American Airlines": {"operator": "AMERICAN"}, 
                 "United Airlines": {"operator": "UNITED"}, 
                 "Lufthansa": {"operator": "LUFTHANSA"}, 
                 "Air France": {"operator": "AIRFRANS"}, 
                 "Aeroflot": {"operator": "AEROFLOT"}, 
                 "China Southern": {"operator": "CHINA SOUTHERN"}, 
                 "Emirates": {"operator": "EMIRATES"}, 
                 "Scandinavian": {"operator": "SCANDINAVIAN"}, 
                 "UPS": {"operator": "United Parcel Service"}, 
                 "ExecJet": {"operator": "EXECJET"}, 
                 "FedEx": {"operator": "Federal Express"}, 
                 "EasyJet": {"operator": "EASY"}, 
                 "Air Canada": {"operator": "AIR CANADA"}, 
                 "KLM": {"operator": "KLM"}, 
                 "Swiss": {"operator": "Swiss International Air Lines"}, 
                 "Air China": {"operator": "AIR CHINA"}, 
                 "Air India": {"operator": "AIRINDIA"}, 
                 "Cathay": {"operator": "CATHAY"}, 
                 "Austrian": {"operator": "AUSTRIAN"}, 
                 "Malaysian": {"operator": "MALAYSIAN"}, 
                 "Indigo Airlines": {"operator": "Indigo Airlines"}, 
                 "Vistajet": {"operator": "Vistajet"}, 
                 "Go Air": {"operator": "Go Air"}, 
                 "SpiceJet": {"operator": "SPICEJET"}, 
                 "Easyjet Switzerland": {"operator": "Easyjet Switzerland Sa"}, 
                 "DHL": {"operator": "Dhl Air"}, 
                 "Australian AirForce": {"operator": "Royal Australian Air Force"}, 
                 "Royal AirForce": {"operator": "Royal Air Force"}, 
                 "Singapore AirForce": {"operator": "Republic Of Singapore Air Force"}, 
                 "Swiss AirForce": {"operator": "Swiss Air Force"}, 
                 "German AirForce": {"operator": "German Air Force"}, 
                 "Indian AirForce": {"operator": "Indian Air Force"}, 
                 "Swiss Jet": {"operator": "Swiss Jet"}, 
                 "Netjets Europe": {"operator": "Netjets Europe"}, 
                 "Netjets": {"operator": "Netjets"}, 
                 "Netjets Sales Inc": {"operator": "Netjets Sales Inc"}, 
                 "Million Air": {"operator": "Million Air"}}

#List of criteria for which results must be aggregated, with column name to use
aggregation_list = [{"col_dest": "EasyJet", "col_source": ["EasyJet", "Easyjet Switzerland"]}, 
                    {"col_dest": "Netjets", "col_source": ["Netjets Europe", "Netjets", "Netjets Sales Inc"]}]



#Class to disable print of progress bar if called in a loop 
class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


#Create MongoDB connection
def createMongo(mongoDB_Db, mongoDB_Col):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[mongoDB_Db]
    mycol = mydb[mongoDB_Col]
    return mycol

#Get distinct values based on multiple fields combinations in mongodb
def get_multiple_distinct(mycol, fields):
    fields_dict = {i: "$" + i for i in fields}
    existing = [i["_id"] for i in mycol.aggregate([{"$group": {"_id": fields_dict}}], allowDiskUse = True)]
    return existing

#Get aircrafts types using manufacturername and model keys
def get_aircrafts_types(mongoDB_Db, mongoDB_Col):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    aircrafts_types = get_multiple_distinct(mycol, ["manufacturername", "model"])
    #Add counts
    for aircraft_type in aircrafts_types:
        aircraft_type["count"] = mycol.count_documents(aircraft_type)
    df = pd.DataFrame(aircrafts_types)
    return df

#Get list of aircrafts owners with counts
def get_aircrafts_owners(mongoDB_Db, mongoDB_Col):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    aircrafts_owners_list = mycol.distinct("owner")
    aircrafts_owners = []
    #Add counts
    for aircraft_owner in aircrafts_owners_list:
        if str(aircraft_owner) == "nan": continue
        aircrafts_owners.append({"owner": aircraft_owner, "count":  mycol.count_documents({"owner": aircraft_owner})})
    df = pd.DataFrame(aircrafts_owners)
    return df

#Get list of aircrafts operators with counts
def get_aircrafts_operators(mongoDB_Db, mongoDB_Col):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    aircrafts_operators_list = mycol.distinct("operator")
    aircrafts_operators = []
    #Add counts
    for aircraft_operator in aircrafts_operators_list:
        if str(aircraft_operator) == "nan": continue
        aircrafts_operators.append({"operator": aircraft_operator, "count":  mycol.count_documents({"operator": aircraft_operator})})
    df = pd.DataFrame(aircrafts_operators)
    return df

#Get top airports
def get_top_airports(mongoDB_Db, mongoDB_Col):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    airports_list = mycol.distinct("origin")
    airports_list += mycol.distinct("destination")
    airports_list = list(set(airports_list))
    airports_stats = []
    #Add counts
    for airport in airports_list:
        if str(airport) == "nan": continue
        num_origin = mycol.count_documents({"origin": airport})
        num_destination = mycol.count_documents({"destination": airport})
        airports_stats.append({"airport": airport, "count": num_origin + num_destination})
    df = pd.DataFrame(airports_stats)
    df.sort_values("count", inplace = True, ascending = False)
    return df

#Get top flights routes
def get_top_routes(mongoDB_Db, mongoDB_Col):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    flights_routes = get_multiple_distinct(mycol, ["origin", "destination"])
    #Exclude routes where origin or destination is NaN (or both are)
    flights_routes = [f for f in flights_routes if str(f["origin"]) != "nan" and str(f["destination"]) != "nan"]
    #Exclude routes where origin is equal to destination
    flights_routes = [f for f in flights_routes if f["origin"] != f["destination"]]
    routes_stats = []
    #Add counts
    for flight_route in flights_routes:
        if str(flight_route) == "nan": continue
        flight_route_txt = "%s-%s" % (flight_route["origin"], flight_route["destination"])
        routes_stats.append({"route": flight_route_txt, "count": mycol.count_documents(flight_route)})
    df = pd.DataFrame(routes_stats)
    df.sort_values("count", inplace = True, ascending = False)
    return df

#Get top aircrafts models
def get_top_models(mongoDB_Db, mongoDB_Col):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    models_list = mycol.distinct("model")
    models_stats = []
    #Add counts
    for model in models_list:
        if str(model) == "nan": continue
        models_stats.append({"model": model, "count": mycol.count_documents({"model": model})})
    df = pd.DataFrame(models_stats)
    df.sort_values("count", inplace = True, ascending = False)
    return df 

#Get top aircrafts manufacturers
def get_top_manufacturers(mongoDB_Db, mongoDB_Col):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    manufacturers_list = mycol.distinct("manufacturername")
    manufacturers_stats = []
    #Add counts
    for manufacturer in manufacturers_list:
        if str(manufacturer) == "nan": continue
        manufacturers_stats.append({"manufacturer": manufacturer, "count": mycol.count_documents({"manufacturername": manufacturer})})
    df = pd.DataFrame(manufacturers_stats)
    df.sort_values("count", inplace = True, ascending = False)
    return df 

#Get number of flights per month between dates for filter
def get_number_flights_month(mongoDB_Db, mongoDB_Col, filter_obj, split_or = False):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    first_date = mycol.find_one(filter_obj, sort=[('day', 1)])["day"]
    first_date = datetime.datetime(first_date.year, first_date.month, 1)
    last_date = mycol.find_one(filter_obj, sort=[('day', -1)])["day"]
    date_range_start = pd.date_range(first_date, last_date, freq='M').tolist()
    date_range_end = pd.date_range(first_date, last_date, freq='MS').tolist()
    date_range = date_range_start + date_range_end
    date_range.sort()
    flights_month = []
    with tqdm(total = int(len(date_range)/2)) as pbar:
        for i in range(1, len(date_range), 2):
            if split_or and list(filter_obj.keys()) == ['$or']:
                count_items = sum([mycol.count_documents(
                    {k: this_filter[k] if k != "day" else {"$gte": date_range[i-1], "$lte": date_range[i]} for k in ["day"] + list(this_filter.keys())})
                    for this_filter in filter_obj["$or"]])
            else:
                filter_dict = filter_obj.copy()
                filter_dict["day"] = {"$gte": date_range[i-1], "$lte": date_range[i]}
                count_items = mycol.count_documents(filter_dict)
            flights_month.append({"month_start": date_range[i-1], 
                                  "month_end": date_range[i], 
                                  "#_flights": count_items})
            pbar.update(1)
    df = pd.DataFrame(flights_month)
    return df
        
#Extract a sample of size n and create a dataframe
def get_sample(mongoDB_Db, mongoDB_Col, n, filter_obj):
    mycol = createMongo(mongoDB_Db, mongoDB_Col)
    if n <= 100000:
        sample = [i for i in mycol.find(filter_obj, {"_id": 0}).limit(n*10)]
        random.shuffle(sample)
        sample = sample [:n]
    else:
        sample = [i for i in mycol.find(filter_obj, {"_id": 0}).limit(n)]
    for i in sample:
        i["latitude_1"] = i["coordinates_1"][1]
        i["longitude_1"] = i["coordinates_1"][0]
        i["latitude_2"] = i["coordinates_2"][1]
        i["longitude_2"] = i["coordinates_2"][0]
        del i["coordinates_1"]
        del i["coordinates_2"]
    df = pd.DataFrame(sample)
    return df

#Get number of flights per month for multiple criteria and return result as dataframe with number of flights for criteria as columns
def get_multiple_num_flights_month(mongoDB_Db, mongoDB_Col, criteria_list, aggregation_list, base = 100):
    results = []
    with tqdm(total = len(criteria_list.keys())) as pbar:
        for criteria in criteria_list.keys():
            with HiddenPrints():
                df = get_number_flights_month(mongoDB_Db, mongoDB_Col, criteria_list[criteria])
            if "month_end" not in df.columns: continue
            df.set_index("month_end", inplace = True)
            df.index = df.index.strftime("%Y-%m-%d")
            df.drop(columns = ["month_start"], inplace = True)
            df.rename({list(df.columns)[0]: criteria}, axis = 1, inplace = True)
            results.append(df)
            pbar.update(1)
    df = pd.concat(results, axis = 1)
    for agg in aggregation_list:
        df[agg["col_dest"]] = df[agg["col_source"]].sum(axis = 1)
        df.drop(columns = [c for c in agg["col_source"] if c != agg["col_dest"]], inplace = True)
    df.replace(np.NaN, 0, inplace = True)
    #Create a second dataframe with rebased values
    df_rebase = df.copy()
    for col in df_rebase.columns:
        df_rebase[col] = np.log(df_rebase[col]) - np.log([i for i in df_rebase[col] if i != 0][0])
        df_rebase[col].replace(np.NaN, 0, inplace = True)
        df_rebase[col] = round(base * np.exp(df_rebase[col]), 2)
    return df, df_rebase

#Format dataframe with number of flights per month per routes by adding airports information
def format_month_routes_df(df_counts_top_routes_rebase, airports_info_file):
    this_df = df_counts_top_routes_rebase.copy().transpose()
    this_df["origin"] = [i.split("-")[0] for i in this_df.index]
    this_df["destination"] = [i.split("-")[1] for i in this_df.index]
    airports_info = pd.read_csv(airports_info_file)
    airports_info.set_index("ident", inplace = True)
    #Create list of all distinct airports to only keep info on those
    all_airports = list(set(list(this_df["origin"]) + list(this_df["destination"])))
    airports_info = airports_info.loc[all_airports]
    #Convert to dictionary to match with fields
    airports_info = airports_info.to_dict(orient = "index")
    this_df["orig_name"] = this_df["origin"].apply(lambda x: airports_info[x]["name"])
    this_df["dest_name"] = this_df["destination"].apply(lambda x: airports_info[x]["name"])
    this_df["orig_type"] = this_df["origin"].apply(lambda x: airports_info[x]["type"])
    this_df["dest_type"] = this_df["destination"].apply(lambda x: airports_info[x]["type"])
    this_df["orig_ctry"] = this_df["origin"].apply(lambda x: airports_info[x]["iso_country"])
    this_df["dest_ctry"] = this_df["destination"].apply(lambda x: airports_info[x]["iso_country"])
    this_df["orig_city"] = this_df["origin"].apply(lambda x: airports_info[x]["municipality"])
    this_df["dest_city"] = this_df["destination"].apply(lambda x: airports_info[x]["municipality"])
    this_df["orig_coord"] = this_df["origin"].apply(lambda x: airports_info[x]["coordinates"])
    this_df["dest_coord"] = this_df["destination"].apply(lambda x: airports_info[x]["coordinates"])
    #Compute year to date delta
    this_df["Jan_YTD"] = (this_df["2020-01-31"] / this_df["2019-01-31"]) - 1
    this_df["Feb_YTD"] = (this_df["2020-02-29"] / this_df["2019-02-28"]) - 1
    this_df["Mar_YTD"] = (this_df["2020-03-31"] / this_df["2019-03-31"]) - 1
    this_df["Apr_YTD"] = (this_df["2020-04-30"] / this_df["2019-04-30"]) - 1
    this_df["May_YTD"] = (this_df["2020-05-31"] / this_df["2019-05-31"]) - 1
    this_df["Jun_YTD"] = (this_df["2020-06-30"] / this_df["2019-06-30"]) - 1
    this_df["Jul_YTD"] = (this_df["2020-07-31"] / this_df["2019-07-31"]) - 1
    this_df["Aug_YTD"] = (this_df["2020-08-31"] / this_df["2019-08-31"]) - 1
    this_df["Sep_YTD"] = (this_df["2020-09-30"] / this_df["2019-09-30"]) - 1
    this_df["Oct_YTD"] = (this_df["2020-10-31"] / this_df["2019-10-31"]) - 1
    #Define columns order, for df with calculations and df with values only
    df_cols = ["origin", "orig_name", "orig_city", "orig_ctry", "orig_type", "orig_coord", 
               "destination", "dest_name", "dest_city", "dest_ctry", "dest_type", "dest_coord", 
               "Jan_YTD", "Feb_YTD", "Mar_YTD", "Apr_YTD", "May_YTD", "Jun_YTD", "Jul_YTD", 
               "Aug_YTD", "Sep_YTD", "Oct_YTD"]
    df_cols_values = ["origin", "orig_name", "orig_city", "orig_ctry", "orig_type", "orig_coord", 
                      "destination", "dest_name", "dest_city", "dest_ctry", "dest_type", 
                      "dest_coord"] + sorted(list(set(this_df.columns).difference(df_cols)))
    #Df with YTD calculations
    this_df_calculations = this_df[df_cols]
    #Df with rebased values
    this_df_values = this_df[df_cols_values]
    return this_df_calculations, this_df_values


#Format dataframe with number of flights per month per airport by adding airports information
def format_month_airports_df(df_counts_top_airports_rebase, airports_info_file):
    this_df = df_counts_top_airports_rebase.copy().transpose()
    airports_info = pd.read_csv(airports_info_file)
    airports_info.set_index("ident", inplace = True)
    #Only keep info on airports in df
    airports_info = airports_info.loc[list(set(this_df.index).intersection(airports_info.index))]
    this_df = this_df.loc[list(set(this_df.index).intersection(airports_info.index))]
    #Convert to dictionary to match with fields
    airports_info = airports_info.to_dict(orient = "index")
    this_df["airport"] = this_df.index
    this_df["name"] = this_df["airport"].apply(lambda x: airports_info[x]["name"])
    this_df["type"] = this_df["airport"].apply(lambda x: airports_info[x]["type"])
    this_df["ctry"] = this_df["airport"].apply(lambda x: airports_info[x]["iso_country"])
    this_df["city"] = this_df["airport"].apply(lambda x: airports_info[x]["municipality"])
    this_df["coord"] = this_df["airport"].apply(lambda x: airports_info[x]["coordinates"])
    #Compute year to date delta
    this_df["Jan_YTD"] = (this_df["2020-01-31"] / this_df["2019-01-31"]) - 1
    this_df["Feb_YTD"] = (this_df["2020-02-29"] / this_df["2019-02-28"]) - 1
    this_df["Mar_YTD"] = (this_df["2020-03-31"] / this_df["2019-03-31"]) - 1
    this_df["Apr_YTD"] = (this_df["2020-04-30"] / this_df["2019-04-30"]) - 1
    this_df["May_YTD"] = (this_df["2020-05-31"] / this_df["2019-05-31"]) - 1
    this_df["Jun_YTD"] = (this_df["2020-06-30"] / this_df["2019-06-30"]) - 1
    this_df["Jul_YTD"] = (this_df["2020-07-31"] / this_df["2019-07-31"]) - 1
    this_df["Aug_YTD"] = (this_df["2020-08-31"] / this_df["2019-08-31"]) - 1
    this_df["Sep_YTD"] = (this_df["2020-09-30"] / this_df["2019-09-30"]) - 1
    this_df["Oct_YTD"] = (this_df["2020-10-31"] / this_df["2019-10-31"]) - 1
    #Define columns order, for df with calculations and df with values only
    df_cols = ["name", "city", "ctry", "type", "coord", "Jan_YTD", "Feb_YTD", "Mar_YTD", 
               "Apr_YTD", "May_YTD", "Jun_YTD", "Jul_YTD", "Aug_YTD", "Sep_YTD", "Oct_YTD"]
    df_cols_values = ["name", "city", "ctry", "type", 
                      "coord"] + sorted(list(set(this_df.columns).difference(df_cols + ["airport"])))
    #Df with YTD calculations
    this_df_calculations = this_df[df_cols]
    #Df with rebased values
    this_df_values = this_df[df_cols_values]
    return this_df_calculations, this_df_values

#Format dataframe with number of flights per month per aircraft model by adding calculations
def format_month_models_df(df_counts_top_models_rebase):
    this_df = df_counts_top_models_rebase.copy().transpose()
    #Compute year to date delta
    this_df["Jan_YTD"] = (this_df["2020-01-31"] / this_df["2019-01-31"]) - 1
    this_df["Feb_YTD"] = (this_df["2020-02-29"] / this_df["2019-02-28"]) - 1
    this_df["Mar_YTD"] = (this_df["2020-03-31"] / this_df["2019-03-31"]) - 1
    this_df["Apr_YTD"] = (this_df["2020-04-30"] / this_df["2019-04-30"]) - 1
    this_df["May_YTD"] = (this_df["2020-05-31"] / this_df["2019-05-31"]) - 1
    this_df["Jun_YTD"] = (this_df["2020-06-30"] / this_df["2019-06-30"]) - 1
    this_df["Jul_YTD"] = (this_df["2020-07-31"] / this_df["2019-07-31"]) - 1
    this_df["Aug_YTD"] = (this_df["2020-08-31"] / this_df["2019-08-31"]) - 1
    this_df["Sep_YTD"] = (this_df["2020-09-30"] / this_df["2019-09-30"]) - 1
    this_df["Oct_YTD"] = (this_df["2020-10-31"] / this_df["2019-10-31"]) - 1
    #Define columns order, for df with calculations and df with values only
    df_cols = ["Jan_YTD", "Feb_YTD", "Mar_YTD", "Apr_YTD", "May_YTD", "Jun_YTD", 
               "Jul_YTD", "Aug_YTD", "Sep_YTD", "Oct_YTD"]
    df_cols_values = sorted(list(set(this_df.columns).difference(df_cols)))
    #Df with YTD calculations
    this_df_calculations = this_df[df_cols]
    #Df with rebased values
    this_df_values = this_df[df_cols_values]
    return this_df_calculations, this_df_values

#From daily covid data compute monthly
def get_monthly_covid_data(covid_data):
    df = pd.read_excel(covid_data)
    monthly_df = df.set_index(["date", "geoId"])
    monthly_df = monthly_df.groupby([pd.Grouper(level='date', freq='M'), pd.Grouper(level="geoId")])["cases", "deaths"].sum()
    monthly_df.reset_index(inplace = True)
    info_df = df.drop(columns = [c for c in df.columns if c not in ['countriesAndTerritories', 'geoId', 'countryterritoryCode', 'popData2019', 'continentExp']]).drop_duplicates()
    monthly_df = monthly_df.join(info_df.set_index("geoId"), on = "geoId")
    monthly_df[["cases_cumul", "deaths_cumul"]] = monthly_df.groupby(['date', 'geoId']).sum().groupby(level=1).cumsum().reset_index()[["cases", "deaths"]]
    monthly_df["date"] = monthly_df["date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    monthly_df["cases_per_100k"] = (monthly_df["cases_cumul"] / monthly_df["popData2019"]) * 1e5
    monthly_df["deaths_per_100k"] = (monthly_df["deaths_cumul"] / monthly_df["popData2019"]) * 1e5
    #Add empty values except countries info to get same date range than flights data
    for country in list(set(monthly_df["geoId"])):
        these_dates = [i for i in pd.date_range("2019-01-30", "2020-11-30", freq = "M").strftime("%Y-%m-%d") if i not in monthly_df[monthly_df["geoId"] == country]["date"].tolist()]
        this_df = pd.DataFrame({"date": these_dates, "geoId": country})
        for col in [c for c in info_df.columns if c != "geoId"]:
            this_df[col] = info_df.set_index("geoId").to_dict(orient = "index")[country][col]
        for col in [c for c in monthly_df.columns if c not in this_df.columns]:
            this_df[col] = 0
        this_df = this_df[monthly_df.columns]
        monthly_df = monthly_df.append(this_df)
    monthly_df["geoId"] = monthly_df["geoId"].replace("UK", "GB")
    monthly_df.sort_values(["date", "geoId"], inplace = True)    
    monthly_df.to_excel("monthly_covid_data.xlsx", index = False, sheet_name = "monthly_covid_data")

#Make df in wide format for Tableau
def pivot_df(excel_path, sheet_name, var_name, value_name = "Flights"):
    t_df = pd.read_excel(excel_path, sheet_name = sheet_name)
    t_df = pd.melt(t_df, id_vars=[list(t_df.columns)[0]], var_name = var_name, value_name = value_name)
    t_df.to_excel("pivot_df.xlsx", index = False)
    
    
    

#Get number of flights per month for selected airlines
def get_flights_per_airlines(mongoDB_Db, mongoDB_Col, criteria_list, aggregation_list):
    df, df_rebase = get_multiple_num_flights_month(mongoDB_Db, mongoDB_Col, criteria_list, aggregation_list)
    writer = ExcelWriter("Num_flights_per_airline_per_month_%s.xlsx" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    df.to_excel(writer, "Values", index = True)
    df_rebase.to_excel(writer, "Rebased values", index = True)
    writer.save()

    
#Get number of flights per month for top airports based on total number of flights
def get_flights_per_airports(mongoDB_Db, mongoDB_Col):
    df = get_top_airports(mongoDB_Db, mongoDB_Col)
    df.to_excel("flights_counts_per_airport.xlsx", index = False)
    top_airports = list(df["airport"])
    criteria_list = {airport: {"$or": [{"origin": airport}, {"destination": airport}]} for airport in top_airports}
    df_counts_top_airports, df_counts_top_airports_rebase = get_multiple_num_flights_month(mongoDB_Db, mongoDB_Col, criteria_list, [])
    df_top_airports_formatted_c, df_top_airports_formatted_v = format_month_airports_df(df_counts_top_airports_rebase, airports_info_file)
    writer = ExcelWriter("Num_flights_per_airport_per_month_full_%s.xlsx" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    df_top_airports_formatted_c.to_excel(writer, "Formatted w calcs", index = True)
    df_top_airports_formatted_v.to_excel(writer, "Formatted w values", index = True)
    df_counts_top_airports.to_excel(writer, "Values", index = True)
    df_counts_top_airports_rebase.to_excel(writer, "Rebased values", index = True)
    writer.save()


#Get number of flights per month per flight route for top 10k routes based on total number of flights
def get_flights_per_routes(mongoDB_Db, mongoDB_Col, top_n = 10000):
    df = get_top_routes(mongoDB_Db, mongoDB_Col)
    df.to_csv("flights_counts_per_route.csv", index = False)    #CSV format as more than 1Mio rows (Excel limit)
    top_routes = list(df["route"])[:top_n]
    criteria_list = {route: {"origin": route.split("-")[0], "destination": route.split("-")[1]} for route in top_routes}
    df_counts_top_routes, df_counts_top_routes_rebase = get_multiple_num_flights_month(mongoDB_Db, mongoDB_Col, criteria_list, [])
    df_top_routes_formatted_c, df_top_routes_formatted_v = format_month_routes_df(df_counts_top_routes_rebase, airports_info_file)
    writer = ExcelWriter("Num_flights_per_route_per_month_top10kroutes_%s.xlsx" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    df_top_routes_formatted_c.to_excel(writer, "Formatted w calcs", index = True)
    df_top_routes_formatted_v.to_excel(writer, "Formatted w values", index = True)
    df_counts_top_routes.to_excel(writer, "Values", index = True)
    df_counts_top_routes_rebase.to_excel(writer, "Rebased values", index = True)
    writer.save()


#Get number of all flights per month (total air traffic)
def get_flights_per_months(mongoDB_Db, mongoDB_Col):
    df_counts_all, df_counts_all_rebase = get_multiple_num_flights_month(mongoDB_Db, mongoDB_Col, {"all": {}}, [])
    writer = ExcelWriter("Global_air_traffic_per_month_%s.xlsx" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    df_counts_all.to_excel(writer, "Values", index = True)
    df_counts_all_rebase.to_excel(writer, "Rebased values", index = True)
    writer.save()


#Get number of flights per aircraft model per month
def get_flights_per_aircrafts(mongoDB_Db, mongoDB_Col):
    df = get_top_models(mongoDB_Db, mongoDB_Col)
    df.to_csv("flights_counts_per_aircraft_model.xlsx", index = False)
    top_models = list(df["model"])
    criteria_list = {model: {"model": model} for model in top_models}
    df_counts_top_models, df_counts_top_models_rebase = get_multiple_num_flights_month(mongoDB_Db, mongoDB_Col, criteria_list, [])
    df_top_models_formatted_c, df_top_models_formatted_v = format_month_models_df(df_counts_top_models_rebase)
    writer = ExcelWriter("Num_flights_per_model_per_month_%s.xlsx" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    df_top_models_formatted_c.to_excel(writer, "Formatted w calcs", index = True)
    df_top_models_formatted_v.to_excel(writer, "Formatted w values", index = True)
    df_counts_top_models.to_excel(writer, "Values", index = True)
    df_counts_top_models_rebase.to_excel(writer, "Rebased values", index = True)
    writer.save()


#Get number of flights per aircraft manufacturer per month for top 10k manufacturers based on total number of flights
def get_flights_per_manufacturers(mongoDB_Db, mongoDB_Col, top_n = 10000):
    df = get_top_manufacturers(mongoDB_Db, mongoDB_Col)
    df.to_csv("flights_counts_per_aircraft_manufacturer.xlsx", index = False)
    top_manufacturers = list(df["manufacturer"])[:top_n]
    criteria_list = {manufacturer: {"manufacturername": manufacturer} for manufacturer in top_manufacturers}
    df_counts_top_manufacturers, df_counts_top_manufacturers_rebase = get_multiple_num_flights_month(mongoDB_Db, mongoDB_Col, criteria_list, [])
    df_top_manufacturers_formatted_c, df_top_manufacturers_formatted_v = format_month_models_df(df_counts_top_manufacturers_rebase)
    writer = ExcelWriter("Num_flights_per_manufacturer_per_month_top1k_%s.xlsx" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    df_top_manufacturers_formatted_c.to_excel(writer, "Formatted w calcs", index = True)
    df_top_manufacturers_formatted_v.to_excel(writer, "Formatted w values", index = True)
    df_counts_top_manufacturers.to_excel(writer, "Values", index = True)
    df_counts_top_manufacturers_rebase.to_excel(writer, "Rebased values", index = True)
    writer.save()