from flask import Flask,jsonify,send_file
from datetime import datetime as dt
import json
import climate_analysis as helper

app = Flask(__name__)

def currentday():
    today = dt.now()
    return today.strftime('%m-%d')

@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    return (
        f"<h1>Welcome to the Hawaiian Climate API!<br/><br/></h1>"
        f"<strong>Available Routes:<br/><br/></strong>"
        f"<strong>General data:<br/><br/></strong>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/><br/>"
        f"<strong>Specific trip data:<br/><br/></strong>"
        f"/api/v1.0/trip/\"startdate as mm-dd\"<br/>"
        f"/api/v1.0/trip/\"startdate as mm-dd\"/\"enddate as mm-dd\"<br/><br/>"
        f"/about<br/><br/>"  
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Server received request for 'Precipitation' page...")
    prcp_df = helper.prcp()
    return (jsonify(prcp_df))

@app.route("/api/v1.0/stations")
def stations():
    print("Server received request for 'Stations' page...")
    stations_df = helper.stations()
    return (jsonify(stations_df))

@app.route("/api/v1.0/tobs")
def tobs():
    print("Server received request for 'Tobs' page...")
    tobs_df = helper.get_tobs()
    return (jsonify(tobs_df))

@app.route("/api/v1.0/trip/<start>")
@app.route("/api/v1.0/trip/<start>/<end>")
def trip(start,end=currentday()):
    print("Server received request for 'Trip' page...")
    daily_normals_df = helper.daily_normals(start,end)
    daily_normals_list = daily_normals_df.to_dict(orient='index')
    return (jsonify(daily_normals_list))

@app.route("/about")
def about():
    print("Server received request for 'About' page...")
    return (
    f"<bold>Welcome to the Hawaiian Climate API!<br/><br/></bold>"
    f"<bold>This page is meant to help you figure out your trip to Hawaii with trip data!<br/><br/></bold>"
    )

if __name__ == "__main__":
    app.run(debug=True)