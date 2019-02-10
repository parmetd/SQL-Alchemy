#!/usr/bin/env python
# coding: utf-8

# # Climate Analysis of Hawaii
# 
# Performing climatic analysis of Hawaii based on the temperature recorded on different stations
# 

# # Dependencies

# In[67]:


# Python SQL toolkit and Object Relational Mapper

# get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect
from sqlalchemy.sql import label
from flask import jsonify


# # Reflect Tables into SQLAlchemy ORM

# In[50]:


engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Create our session (link) from Python to the DB
session = Session(engine)


# In[5]:


# We can view all of the classes that automap found
Base.classes.keys()


# In[6]:


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


# # Exploratory Climate Analysis

# In[8]:


inspector = inspect(engine)

columns = inspector.get_columns('measurement')
for column in columns:
    print(column["name"], column["type"])


# In[9]:


columns = inspector.get_columns('station')
for column in columns:
    print(column["name"], column["type"])


# In[10]:


engine.execute('SELECT * FROM measurement LIMIT 10').fetchall()


# In[11]:


engine.execute('SELECT * FROM station LIMIT 10').fetchall()


# In[12]:


data_measurement = pd.read_sql("SELECT * FROM measurement", engine.connect())
data_measurement.head()


# In[13]:


data_station = pd.read_sql("SELECT * FROM station", engine.connect())
data_station.head()


# In[14]:


data_station['name'].value_counts()


# In[15]:


data_station.count()


# In[16]:


data_measurement['date'].value_counts()


# In[17]:


data_measurement.count()


# In[18]:


data_measurement.isna().sum()


# In[19]:


data_station.isna().sum()


# In[20]:


session.query(func.count(Measurement.date)).all()


# In[21]:


# Earliest Date
session.query(Measurement.date).order_by(Measurement.date).first()


# In[22]:


#Last Date
session.query(Measurement.date).order_by(Measurement.date.desc()).first()


# # Query Analysis

# In[23]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results
date_filter = session.query(Measurement.date, Measurement.prcp).    filter(Measurement.date > '2016-08-23').    order_by(Measurement.date).statement

df_prcp = pd.read_sql_query(date_filter, session.bind)
df_prcp.head(10)


# In[24]:


df_prcp.isna().sum()


# In[25]:


# Removing NaN
df_prcp_dropped = df_prcp.dropna()
df_prcp_dropped.count()


# In[26]:


df_prcp_dropped.head()


# In[141]:


# Use Pandas Plotting with Matplotlib to plot the data
# df_prcp_dropped['date']  = pd.to_datetime(df_prcp_dropped['date'])
# plt.plot(df_prcp_dropped['date'], df_prcp_dropped['prcp'])
# df_prcp_dropped.plot(kind="line",linewidth=4,figsize=(15,10))
plt.figure(figsize=(15,10))
plt.plot('date','prcp',data=df_prcp_dropped, linewidth=4)
plt.style.use('fivethirtyeight')
plt.xlabel("Time", fontsize = 18)
plt.ylabel("Rainfall (inches)", fontsize = 18)
plt.title("Precipitation over 12 months in Hawaii", fontsize = 24)
plt.ylim(0, max(df_prcp_dropped['prcp']+1))
plt.xticks(rotation='90')
plt.tight_layout()
plt.show()

# Saving visualization as an image.
plt.savefig("Precipitation_year.png")


# In[28]:


df_prcp_dropped.describe()


# In[29]:


print(len(df_prcp_dropped[df_prcp_dropped['prcp'] != 0]))
print(len(df_prcp_dropped[df_prcp_dropped['prcp'] != 0].dropna()))


# In[30]:


round(df_prcp_dropped[df_prcp_dropped['prcp'] != 0].mean(),2)


# In[31]:


df_prcp_dropped['prcp'].min()


# In[32]:


no_rain_count_c = df_prcp_dropped.groupby('date').sum()['prcp']
no_rain_count = no_rain_count_c[no_rain_count_c == 0].count()
no_rain_count


# In[46]:


# Use Pandas to calcualte the summary statistics for the precipitation data
rain_min = df_prcp_dropped['prcp'].min()
rain_max = df_prcp_dropped['prcp'].max()
total = df_prcp_dropped['prcp'].sum()
average = round(df_prcp_dropped['prcp'].mean(),2)
average_if_rain = round(df_prcp_dropped[df_prcp_dropped['prcp'] != 0].mean(), 2)
#no_rain_count = df_prcp_dropped[df_prcp_dropped['prcp'] == 0]['prcp'].count()
rainy_days = 365 - no_rain_count
responses_dict = {'Rainy Days': rainy_days, 'Number of Days with No Rain': no_rain_count,'Highest Rainfall Day': rain_max, 'Average Rainfall Per Day': average, 'Average Rain per Rainstorm': average_if_rain, 'Total Rainfall': total}
summary_df = pd.DataFrame(responses_dict)
summary_df.rename(index={"prcp": "Precipitation"}, inplace=True)
summary_df.to_csv('rainfall_summary.csv')

summary_df


# In[34]:


# Design a query to show how many stations are available in this dataset?
station_boolean = session.query(Measurement.station).distinct().count()== session.query(Station.station).distinct().count()
if station_boolean:
    num_station = session.query(Measurement.station).distinct().count()
    print(f'The number of stations = {num_station}')
else:
    print('your database is not NSYNC')


# In[35]:


# What are the most active stations? (i.e. what stations have the most rows)?
# List the stations and the counts in descending order.
desc_observations = engine.execute('select station, count(station) from measurement group by station order by count(station) desc').fetchall()

busiest = desc_observations[0][0]    
print("The busiest Station was",busiest,"with",desc_observations[0][1],"weather observations.")
print("----------------------------------------------------------------------------------------")
print("Here are all of the Stations (in descending order) with their number of observations:")
for station, count in desc_observations:
    print("Station",station,"had",count, "weather observations.")


# In[41]:


# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature most active station?

temp_min, temp_max, temp_avg = session.query(func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).                filter(Measurement.station==busiest).all()[0]

temp_avg_r = round(temp_avg,1)

print (f"The lowest temperature observed is {temp_min}. \nThe highest temperature observed is {temp_max}. \nThe average temperature observed is {temp_avg_r}.")


# In[44]:


# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram
station_id = desc_observations[0][0]
station_name = session.query(Station.name).filter(Station.station==desc_observations[0][0]).all()
print(f'The highest number of observations come from {station_name[0][0]}')

tobs_query = session.query(Measurement.tobs, Measurement.station).filter(Measurement.date >'2016-08-23', Measurement.date <='2017-08-23').filter(Measurement.station==station_id).order_by(Measurement.date).statement 

tobs_df = pd.read_sql(tobs_query, engine).dropna()
tobs_df.head(5)

x = tobs_df['tobs']
plt.figure(figsize=(12,8))
plt.hist(x, bins=12)
plt.xlabel(r'Temperature ($^\circ$F)')
plt.ylabel('Frequency')
plt.savefig("Observ_Dist.png")
plt.show()


# # Temperature Analysis

# In[79]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates with a graph.

# def calc_temps(start_date, end_date):
#     """TMIN, TAVG, and TMAX for a list of dates.
    
#     Args:
#         start_date (string): A date string in the format %Y-%m-%d
#         end_date (string): A date string in the format %Y-%m-%d
        
#     Returns:
#         TMIN, TAVE, and TMAX
#     """
    
#     return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
#         filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# # function usage example
# print(calc_temps('2012-02-28', '2012-03-05'))

def calc_temps(startdate, enddate):
    start = startdate - timedelta(days=365)
    end = enddate - timedelta(days=365)
    
    temp_vac = session.query(label('max_temp',func.max(Measurement.tobs)),                                     label('min_temp',func.min(Measurement.tobs)),                                     label('avg_temp',func.avg(Measurement.tobs))).                                     filter(Measurement.date >= start).                                     filter(Measurement.date <= end)
    
    Max_temp = temp_vac[0].max_temp
    Min_temp = temp_vac[0].min_temp
    Avg_temp = temp_vac[0].avg_temp
    
    yerror = Max_temp - Min_temp
    
    barvalue = [Avg_temp]
    xvals = range(len(barvalue))
    plt.rcParams.update({'font.size': 12})
    
    fig,ax = plt.subplots(figsize=(5,12))
    ax.bar(xvals, barvalue, yerr=yerror, color='salmon',alpha=0.5)
    ax.set_xticks([1]) 
    plt.xlabel("Vacation Time Period")
    plt.ylabel("Temperature")
    plt.title("Trip Average Temperature")
    plt.tight_layout()
    plt.savefig("trip_avg.png")
    
    plt.show()


# In[80]:


# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.
calc_temps(dt(2016,8,23), dt(2017,8,24))


# In[110]:


# Calculate the rainfall per weather station for your trip dates using the previous year's matching dates.
# Sort this in descending order by precipitation amount and list the station, name, latitude, longitude, and elevation

def rainfall(startdate,enddate):
    start = startdate - timedelta(days=365)
    end = enddate - timedelta(days=365)
    
    rainfall_per_station = session.query(Station.name,Station.station,Station.latitude,Station.longitude,label('avg_rainfall',func.avg(Measurement.prcp))).                    filter(Measurement.station == Station.station).                    filter(Measurement.date >= start).                    filter(Measurement.date <= end).                    group_by(Station.name,Station.station).order_by(func.avg(Measurement.prcp))
    
    df = pd.DataFrame(query_to_dict(rainfall_per_station))
       
    return df


def query_to_dict(res):
    res_dict = []
    for rec in res:
        res_dict.append(rec)
    
    return res_dict


# In[108]:


rainfall(dt(2016,8,23), dt(2017,8,24))


# In[118]:


# def daily_normals(date):
#     """Daily Normals.
    
#     Args:
#         date (str): A date string in the format '%m-%d'
        
#     Returns:
#         A list of tuples containing the daily normals, tmin, tavg, and tmax
    
#     """
    
#     sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
#     return session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()
    
# daily_normals("01-01")

def daily_normals(startdate, enddate):
    df = pd.DataFrame()
    session = Session(engine)
    compstart = dt.strptime(startdate,"%m-%d") - timedelta(days=365)
    compend = dt.strptime(enddate,"%m-%d") - timedelta(days=365)
    
    compstart = compstart.replace(year = 2017)
    compend = compend.replace(year = 2017)
    
    print(f"compstart-{compstart} : compend-{compend}")
    
    daily_temperatures = session.query(Measurement.date,label('tmax',func.max(Measurement.tobs)),                                      label('tmin',func.min(Measurement.tobs)),
                                      label('tavg',func.avg(Measurement.tobs)),).\
                                      filter(Measurement.date >= compstart).\
                                      filter(Measurement.date <= compend).\
                                      group_by(Measurement.date).order_by(Measurement.date)
    
    df = pd.DataFrame(query_to_dict(daily_temperatures))
    
    print(f"dataframe columns - {df.columns}")
    
    if('date' in df.columns):
        df = df.set_index('date')
    
    return df


# In[119]:


daily_normals('01-01','01-02')


# In[123]:


def show_daily_normals():
    daily_normals_df = daily_normals('01-01','01-10')
    daily_normals_df.plot(kind='area',stacked=False,figsize=(15,10),alpha=0.5,cmap = plt.cm.get_cmap('Paired'))

    plt.xlabel("Date")
    plt.ylabel("Temperature")
    plt.title(f"Trip - Daily Normals")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig("trip_daily_normals.png")

    plt.show()


# In[124]:


show_daily_normals()


# In[130]:


def prcp():

    # select all the data and precipitation 
    results = session.query(Measurement.date, Measurement.prcp).        order_by(Measurement.date).all()

    # convert results to dictionary and return json file
    prcp_dict = {}

    for date, prcp in results:
        prcp_dict[date] = prcp
        
    return prcp_dict


# In[129]:


def stations():
    # Station column names
    name_list = []
    inspector = inspect(engine)
    columns = inspector.get_columns('station')
    for column in columns:
        name_list.append(column["name"])
    name_list.remove('id')

    station_dict = {}
    for result in session.query(Station):
        result_dict = result.__dict__
        result_dict_clean = {key: result_dict[key] for key in name_list }
        station_dict['id_'+str(result_dict['id'])] = result_dict_clean
    
    return station_dict


# In[128]:


def get_tobs():
    current_time = datetime.now()

    past_year = current_time - timedelta(days=365)
    

    temp = session.query(Measurements.date,Measurements.tobs).                        filter(Measurements.date > past_year).all()

    station_measures = []
    for measure in temp:
        station_measures.append(measure._asdict())

    station_measures_df = pd.DataFrame.from_records(station_measures)

    station_measures_df = station_measures_df.set_index('date')


    return station_measures_df

