# some_file.py
import sys
import os
import json
import glob

import logging

from loguru import logger

import pandas as pd
import numpy as np

import datetime
import time
from datetime import timedelta  

import stravaio 

from statist import Statist

def login(user_token):
	# Public identifier for apps
	STRAVA_CLIENT_ID = 9402
	
	# Secret known only to the application and the authorization server
	STRAVA_CLIENT_SECRET = "7960741d3c1563506e073c364e71473c5da1405c"
	
	ACCESS_TOKEN = stravaio.get_access_token(
		port=8000,
		client_id=STRAVA_CLIENT_ID,
		client_secret=STRAVA_CLIENT_SECRET,
		user_token=user_token
	)	
	return ACCESS_TOKEN
	
def logoff(access_token):	
	stravaio.deauthorize(access_token)
	
def cleanDb(athlete_id):
	stat = Statist(logger)
	return stat.cleanDb(athlete_id)	
	
	
def getAthlete(access_token):
	access = stravaio.StravaIO(access_token=access_token)
	
	'''
	endDate = datetime.datetime.now() +  timedelta(hours=24) 
	startDate = endDate - timedelta(days=31)
	#startDate = endDate - timedelta(days=31*12*15)
	act.retreive_strava_activities(startDate, endDate)	'''
		

	athlete = access.get_logged_in_athlete()

	if athlete is not None:	
		athlete.store_locally()
		athlete = athlete.to_dict()
			
		logger.debug("athlete : id " + str(athlete["id"]) + ", " + athlete["firstname"] + " " + athlete["lastname"] + " from " + athlete["city"] + ", " + str(athlete["weight"]) + "kg")
	else:
		logger.debug(f"Error in calling Strava API for acces token <" + access_token + ">")	
		athlete = None
		
	return athlete
	
	
	
def get_one_page_activities(access_token, athlete_id, startdate, enddate, page = 0, per_page=100, list_activities=None):
	"""get_one_page_activities

	Returns
	-------
	list including the dates of the first and the last activity and the number of activity
	"""
	
	logger.debug("get_one_page_activities from <" + str(startdate) + "> to <" + str(enddate) + ">")

	access = stravaio.StravaIO(access_token=access_token)
	
	# Get list of athletes activities since a given date (after) given in a human friendly format.
	return access.get_one_page_activities(after=startdate,before=enddate, page=page,per_page=per_page, list_activities = list_activities)
	

def ComputeDatas(list_activities, athlete_id, startdate, enddate):

	startdate = startdate.replace(tzinfo=None)
	enddate = enddate.replace(tzinfo=None)	
	
	stat = Statist(logger)
	return stat.ComputeDatas(list_activities, athlete_id, startdate, enddate)


def RetreiveFromDateInterval(access_token, athlete_id, startdate, enddate):
	"""RetreiveFromDateInterval

	Returns
	-------
	list including the dates of the first and the last activity and the number of activity
	"""
	
	logger.debug("RetreiveFromDateInterval from <" + str(startdate) + "> to <" + str(enddate) + ">")

	access = stravaio.StravaIO(access_token=access_token)
	
	# Get list of athletes activities since a given date (after) given in a human friendly format.
	# Kudos to [Maya: Datetimes for Humans(TM)](https://github.com/kennethreitz/maya)
	# Returns a list of [Strava SummaryActivity](https://developers.strava.com/docs/reference/#api-models-SummaryActivity) objects
	list_activities = access.get_logged_in_athlete_activities(after=startdate,before=enddate, page=0,per_page =100 )

		
	'''strava_dir = stravaio.dir_stravadata()
	
	activities_dir = os.path.join(strava_dir, f"summary_activities_{athlete_id}")
	if not os.path.exists(activities_dir):
		os.mkdir(activities_dir)

	streams = []

	# Remove all files from dir "summary_activities_{athlete_id}"
	files = glob.glob(activities_dir + '/*')
	for f in files:
		os.remove(f)
	 
	# Obvious use - store all activities locally
	#for a in list_activities:		
		#store stream if not exist yet
		if not self.isStreamStored(a.id):
			streams = self.client.get_activity_streams(a.id, self.athlete.id, False) #local = False to retreive data from Strava
			streams.store_locally()
			streams = pd.DataFrame(streams.to_dict())
		else:
			dir_streams = os.path.join(dir_stravadata(), f"streams_{self.athlete.id}")
			f_name = f"streams_{a.id,}.parquet"
			f_path = os.path.join(dir_streams, f_name)
			if f_path in glob.glob(f_path):
				streams = pd.read_parquet(f_path)
		
		print("stream id : ",a.id)'''
				

	startdate = startdate.replace(tzinfo=None)
	enddate = enddate.replace(tzinfo=None)
	
	
	stat = Statist(logger)
	return stat.ComputeDatas(list_activities, athlete_id, startdate, enddate)


def isStreamStored(activity_id):
	strava_dir = dir_stravadata()
	streams_dir = os.path.join(strava_dir, f"streams_{self.athlete.id}")
	streams_dir = os.path.join(streams_dir, f"streams_{activity_id}.parquet")
	print(streams_dir)
	if os.path.isfile(streams_dir):
		return True
	else:
		return False




def getStatBy(byWhat, id, listActivityType, listDataType):	
	# create logger
	logger = logging.getLogger('')
	logger.setLevel(logging.DEBUG)
	
	stat = Statist(logger)

	#athlete_id = 134706
	
	return stat.Stat_dist_by(byWhat, id,listActivityType, listDataType)	
	
	#wait = input("PRESS ENTER TO CONTINUE.")
	
	
		
	
def getStatAnnual(id, listActivityType, listDataType, listObjective):	
	# create logger
	logger = logging.getLogger('')
	logger.setLevel(logging.DEBUG)
	
	stat = Statist(logger)
	
	#athlete_id = 134706
	
	return stat.Stat_dist_annual(id, listActivityType, listDataType,listObjective)
	
	#wait = input("PRESS ENTER TO CONTINUE.")
	

if __name__ == "__main__":
	run()



