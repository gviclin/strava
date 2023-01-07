from django.shortcuts import render
from django.utils import timezone
from django.shortcuts import render, get_object_or_404
from django.shortcuts import redirect, get_object_or_404
from django.http import HttpResponse, HttpResponseRedirect,JsonResponse, StreamingHttpResponse, Http404
from django.conf import settings
from .models import User
from .forms import PostSettings

from datetime import datetime, tzinfo, date
import pytz

from tabulate import tabulate
#import pprint 

from loguru import logger
#from django.utils.timezone import make_aware

import os
import sys
import json
import glob
import pandas as pd
import numpy as np
import re

import plotly.express as px
import plotly.graph_objs as go
import plotly.offline as offline

from stravaApp.stravaInterface import *

from django.views.decorators.csrf import csrf_exempt


# Generator for send sync progress infos to the client
def event_stream(user, request, startDate, endDate):
	response = {}
	list_activities= []
	page=1	
	progressValueActivities = 0
	progressValueCompute = 0
	
	while True:
		if progressValueCompute==100:
			break
				
		if progressValueCompute==10:
			range = ComputeDatas(list_activities, user.user_id, startDate, endDate)		
			
			# Retreive from Strava Site finished. Update session			
			if range:
				#store the date range in db
				user = User.objects.get(user_id=int(user.user_id))
				logger.debug("-> result : activity from " + str(range[0]) + " to " + str(range[1]) + ". Number " + str(range[2]))
				
				result = "Synchronisation from of <" + str(range[2]) + "> activities from <" +  str(range[0]) + "> to <" + str(range[1]) + ">"

				
				if range[0]:
					user.first_activity_date = timezone.make_aware(range[0], timezone=timezone.utc)
				else:
					user.first_activity_date = None
				if range[1]:
					user.last_activity_date = timezone.make_aware(range[1], timezone=timezone.utc)
				else:
					user.last_activity_date = None
				
				user.act_number = range[2] if range[2] else 0
				user.save()
				progressValueCompute = 100
		else:

			
			nbRetreived=0
			try:
				nbRetreived = get_one_page_activities(user.access_token, user.user_id, startDate, endDate, page, 100, list_activities)
			
				#range = RetreiveFromDateInterval(access_token, user.user_id, startDate, endDate)
			except Exception as e:
				logger.debug("Exception  : "+ str(e))
				range = None
				request.session['ACCESS_TOKEN'] = None
				
				
			if nbRetreived==0:
				#no more datas
				page = 0
				progressValueActivities = 100				
				progressValueCompute = 10				
				
			else:
				progressValueActivities = page * 10
				if progressValueActivities > 100:
					progressValueActivities = 100
				page+= 1
			

		logger.debug("StravaSync progressValueActivities <" + str(progressValueActivities) +">. progressValueCompute <" + str(progressValueCompute) + ">")
		response["result"] = "result"

			
		response["progressValueActivities"] = progressValueActivities
		response["progressValueCompute"] = progressValueCompute
		str1 = str(response).replace("'", '"') # dirty code  : javascript JSON supports supports " and not single quote '
		yield 'data: ' + str1  +' \n\n'
		#yield 'data: The server time is: %s\n\n' % timezone.now()

		#time.sleep(0.1)


def viewStravaSync(request):
	logger.debug("======> viewStravaSync. URL <" + request.path + ">")
	
	#check if logged
	user = getUserModel(request)
	if user: 
		isLogged =  True			

		#Retreive strava datas
		#endDate = timezone.make_aware(datetime.datetime.utcnow() +  timedelta(hours=24),timezone=timezone.utc)
		endDate = datetime.datetime.utcnow() +  timedelta(hours=24)
		#logger.debug("endDate : " + endDate.tzname()) 			
		
		if user.last_activity_date is not None:		
			#logger.debug("--> last_activity_date is not null")			
			startDate = user.last_activity_date  +  timedelta(seconds=1)
			#unset the timezone !!!
			startDate = startDate.replace(tzinfo=None)
		else:
			#logger.debug("--> last_activity_date is null") #timezone=timezone.utc
			startDate = datetime.datetime(2000, 1, 1, 0, 0 ,0)
	
						
		return StreamingHttpResponse(event_stream(user, request, startDate, endDate), content_type='text/event-stream')
	else:
		 # ?
		 to = 5

@csrf_exempt
def sync_ajax(request):
	logger.debug("======> sync_ajax. URL <" + request.path + ">")
	activityType = "No value"
	statType = "No value"
	response = {"log":""}
	html = ""
	listType = []
	
	#check if logged
	access_token = request.session.get('ACCESS_TOKEN', None) 
	
	if access_token and request.method == "POST":		
		response["log"] 
	
	return JsonResponse(response, status = 200)
	
def getUserModel(request):
	user = None
	
	#check if logged
	user_id = request.session.get('user_id', None)
	if  user_id is not None:		
		# retreive user table
		user = User()	
		try:
			user = User.objects.get(user_id=user_id)
		except user.DoesNotExist:
			user = None

	return user

def viewSettingPost(request):
	logger.debug("======> viewSettingPost. URL <" + request.path + ">")
	actif = 3	# login active
	isLogged = True #logged because clic on setting bouton
	name = request.session.get('name', 'no_name') 

	if os.environ.get('DEV'):
		dev = True
	else:
		dev = False
		
	user = getUserModel(request)
	if user: 
		if request.method == "POST":
			form = PostSettings(request.POST)
			if form.is_valid():
				data = form.save(commit=False)
				user.year_run_objective = data.year_run_objective
				user.year_ride_objective = data.year_ride_objective
				user.save()
				return redirect('viewSettingPost')
		
		else:
			form = PostSettings(instance=user)
	else:
		raise Http404("User is not connected !!!!")
			
	return render(request, 'settingsStrava.html', {'form': form, 'actif': actif, 'isLogged': isLogged, 'name': name})

@csrf_exempt
def post_ajax(request):
	activityType = "No value"
	statType = "No value"
	dataType = "No value"
	response = {"log":""}
	html = ""
	listActivityType = []
	listDataType = []
	
	#check if logged
	user = getUserModel(request)
	
	access_token = user.access_token
	
	if user and user.access_token and user.updated_date:
		if request.method == "POST":
			if 'activityType' in request.POST:
				activityType = request.POST['activityType']	
				response["log"]  = "Activity type : <" + activityType + ">"
			if 'pageType' in request.POST:
				statType = request.POST['pageType']	
				response["log"]  = response["log"] + " Page type : <" + statType  +">"
			if 'dataType' in request.POST:
				dataType= request.POST['dataType']	
				response["log"]  = response["log"] + " Data type : <" + dataType  +">"
	
		logger.debug("======> post_ajax. URL <" + request.path + ">. Stat type <" + statType + ">. Activity type <" + activityType + ">. Data Type <" + dataType + ">")
			
		if statType=="month":
			listActivityType 	= GetListActivityList(activityType)	
			listDataType	 	= GetListDataList(dataType)	
			
			if len(listActivityType)>0:
				df = getStatBy("month", user.user_id, listActivityType, listDataType)
				if not df.empty:	
					html = 	df.to_html(index_names=True)
				else:
					logger.debug("List is empty !");
					html=""
		
		if statType=="week":
			listActivityType 	= GetListActivityList(activityType)	
			listDataType	 	= GetListDataList(dataType)	
			
			if len(listActivityType)>0:
				df = getStatBy("week", user.user_id, listActivityType, listDataType)
				if not df.empty:	
					html = 	df.to_html(index_names=True)
				else:
					logger.debug("List is empty !");
					html=""
		
				
		elif statType=="year":
			listActivityType 	= GetListActivityList(activityType)
			listDataType	 	= GetListDataList(dataType)			
			listObjective 		= GetListObjectiveList(activityType, user)
			
			if len(activityType)>0:
				html = generateGraph(user.user_id, listActivityType, listDataType, listObjective)		
			else:
				logger.debug("List is empty !");
				
		elif statType=="setting":
			html = "Settings"
			
							
		elif statType=="refresh":	
			user.first_activity_date = None
			user.last_activity_date = None
			user.first_activity_date = None
			user.act_number = 0
			user.save()	
			cleanDb(user.user_id)
			html = ""
			
		elif statType=="delete":	
			user.first_activity_date = None
			user.last_activity_date = None
			user.first_activity_date = None
			user.act_number = 0
			user.save()	
			cleanDb(user.user_id)
			html = ""
			
		elif statType=="logout":
			#logoff		
			if 	access_token is not None:
				#logger.debug(f"logout. Access token <" + access_token + ">")		
				user.updated_date = None
				user.access_token = None
				user.save()	

				
			html = ""	
			
		response["data"] = html
	
	else:
		# not logged
		response["log"]  = "Not logged"
		return JsonResponse(response, status = 500)


	return JsonResponse(response, status = 200)


def GetListActivityList(activityType):
	listActivityType =[]
	if activityType.find("run")!=-1:
		listActivityType.append("Run")
	if activityType.find("ride")!=-1:
		listActivityType.append("Ride")
		listActivityType.append("VirtualRide")
	if activityType.find("walk")!=-1:
		listActivityType.append("Walk")
		listActivityType.append("Hike")
		
	return listActivityType
	
def GetListDataList(activityType):
	listDataType =[]
	if activityType.find("distance")!=-1:
		listDataType.append("distance")
	if activityType.find("time")!=-1:
		listDataType.append("time")
	if activityType.find("elevation")!=-1:
		listDataType.append("elevation")

	return listDataType
	
	
def GetListObjectiveList(activityType, user):
	listObjectiveType = []
	if activityType.find("run")!=-1:
		if user:
			listObjectiveType = [user.year_run_objective]
		else:
			listObjectiveType = [500]
		#objList = [1400,1600]

	if activityType.find("ride")!=-1:
		if user:
			listObjectiveType = [user.year_ride_objective]
		else:
			listObjectiveType = [500]
		#objList = [6000,7000]
		
	if activityType.find("walk")!=-1:
		listObjectiveType = []
		
	return listObjectiveType


# View login: callback from strava when user logs on it
def viewLogin(request):
	logger.debug("======> viewLogin. URL <" + request.path + ">")
	actif = 3	
	if os.environ.get('DEV'):
		dev = True
	else:
		dev = False
		
	isLogged = False
	name = "Login"
	
	#url = request.path
	#Get param :
	if 'code' in request.GET:
		user_code = request.GET['code']	
		
		logger.debug(f" user_code <" + str(user_code) + ">")		
		
		access_token = login(user_code)
		
		logger.debug(f"New access_token <" + str(access_token) + ">")
		
		if access_token is not None:			
			isLogged = True	
	
	if isLogged:	
		#retrieve the athlete infos and save it in db
		athlete = getAthlete(access_token)	
		if athlete:	
			# store athlete id in session		
			request.session['user_id'] = int(athlete["id"])	
			# store athlete infos in DB
			user = User()
			try:
				user = User.objects.get(user_id=int(athlete["id"]))
			except user.DoesNotExist:
				user = None
			
			if not user:
				logger.debug(f"if not user:")
				user = User()
				user.user_id = athlete["id"]
				
			#user.django_user = request.user
			user.access_token = access_token
			user.firstname = athlete["firstname"]
			user.lastname = athlete["lastname"]
			user.weight = athlete["weight"]
			user.sex = athlete["sex"] if athlete["sex"] else ""
			user.country = athlete["country"]
			user.state = athlete["state"]
			user.city = athlete["city"]
			user.follower_count = int(athlete["follower_count"])
			user.friend_count = int(athlete["friend_count"])
			user.measurement_preference = athlete["measurement_preference"]
			user.ftp = int(athlete["ftp"] if athlete["ftp"] else -1)
			user.updated_date = timezone.now()
			user.strava_creation_date = timezone.make_aware(datetime.datetime.strptime(athlete["created_at"], '%Y-%m-%dT%H:%M:%SZ'),timezone=timezone.utc)
			user.save()
		else:
			logoff(access_token)
			request.session['user_id'] = None
			#html = f"Error in calling Strava API"
			name ="Login"		
		
		return index(request, actif = 1)
	else:
		html = "Login failed !! "
		logger.debug(f" Login failed !")

	#logger.debug(f"session_key : "+ request.session.session_key)
		
	return render(request, 'baseStrava.html', locals() )


	
def index(request, actif = 1):
	logger.debug("======> index. URL <" + request.path + ">")
	# By default, stat by month
	#actif = 1	
	if os.environ.get('DEV'):
		dev = True
	else:
		dev = False
				
	#check if logged
	user = getUserModel(request)
	if user: 		
		update_dt = user.updated_date
		logger.debug(f"updated_date : {str(update_dt)}")
		if update_dt:
			update_dt = update_dt.replace(tzinfo=None)
			if ( update_dt + datetime.timedelta(hours=1) > datetime.datetime.utcnow()):
				logger.debug(f"updated_date ok")
				name = user.firstname + " " + user.lastname + " <i class=\"fa fa-caret-down\"></i>"
				request.session['name'] = name	
				isLogged =  True
			else:
				logger.debug(f"updated_date ko")
				logger.debug(str(update_dt) + " / " + str(datetime.datetime.utcnow()))
				isLogged =  False	
		else:
			isLogged =  False	
	else:
		logger.debug("Error, user model does not exists")
		isLogged =  False

		
	return render(request, 'baseStrava.html', locals() )
	

def generateGraph(id, listActivityType, listDataType, objList):
	plot_div = ""	
	dataType =""
	
	if len(listDataType):
		if listDataType[0] == "distance":
			dataType = "distance"
			layout_title = "Cumulative distance (" + listActivityType[0] + ")"
			yaxis_title = "Distance (km)"
			yaxis_tickformat1=""
			tickmode1="auto"
			dtick1 = 100 if "Run" in listActivityType else 1000
			nticks1 = 20
			#https://github.com/d3/d3-3.x-api-reference/blob/master/Formatting.md#d3_format
			#https://github.com/d3/d3-3.x-api-reference/blob/master/Time-Formatting.md#format
			hovertemplate1 = '%{x|%d/%m}/%{data.name}<br>%{y:.1f} kms<br>Activity : %{customdata:.1f} kms<extra></extra>'
		elif listDataType[0] == "time":			
			dataType = "moving_time"
			layout_title = "Cumulative moving time (" + listActivityType[0] + ")"
			yaxis_title = "Time (hours)"
			yaxis_tickformat1=".0f hours" #'%e days'
			tickmode1="auto"
			dtick1 = 0
			nticks1 = 0
			#hovertemplate1 = '%{x|%d/%m}/%{data.name}<br>%{y|(%e days %X} <br>Activity : %{customdata|%X} <extra></extra>'
			hovertemplate1 = '%{x|%d/%m}/%{data.name}<br>%{y:.1f} hours <br>Activity : %{customdata|%X} <extra></extra>'
		elif listDataType[0] == "elevation":
			dataType = "total_elevation_gain"
			layout_title = "Cumulative elevation gain (" + listActivityType[0] + ")"
			yaxis_title = "Elevation gain (meter)"
			yaxis_tickformat1=""
			tickmode1="auto"
			dtick1 = 0
			nticks1 = 0
			hovertemplate1 = '%{x|%d/%m}/%{data.name}<br>%{y:.0f} meters<br>Activity : %{customdata:.0f} meters<extra></extra>'
	
		df = getStatAnnual(id, listActivityType, dataType, objList)
		
		if listDataType[0] == "time":
			#convert to time
			# time delta issue : https://github.com/plotly/plotly.py/issues/801
			'''df["cumul"] = pd.to_timedelta(df["cumul"], unit='s') + pd.to_datetime('1970/01/01')'''
			df["moving_time"] = pd.to_timedelta(df["moving_time"], unit='s') + pd.to_datetime('1970/01/01')
			df["cumul"] = df["cumul"].apply(lambda value: value / 3600)
					

		
		'''print(str(df))
		print(df.dtypes)
		print(df.index)'''
		#print(tabulate(df, headers='keys', tablefmt='psql'))
						
		if not df.empty:	
			df["year"] = df["year"].apply(str)
			listLines = df["year"].unique()
			
			maxCumul = df["cumul"].max()
			
			graphList = []
			for line in listLines:
				#print("listLine : " + str(line))
				filter = df["year"] == line
				dfFilter = df[filter]	
				
				# objectif line
				if line.find("km") != -1:
					graph = go.Scatter(
						x=dfFilter["start_date"],
						y=dfFilter["cumul"],
						name=line,
						opacity=1,
						mode="lines",
						line=dict(dash="dashdot", width=3), # dot, dash, dashdot
						text = line,
						marker=dict(
							symbol="circle",
							size=6,
							line=dict(width=0,
								color='DarkSlateGrey'
								),
						),		
						hovertemplate = 'Objective : %{data.name}<br>%{x|%d/%m}<br>%{y:.1f} kms<extra></extra>',			
					)
				else:
					# Normal line
					graph = go.Scatter(
						x=dfFilter["start_date"],
						y=dfFilter["cumul"],
						customdata=dfFilter[dataType],
						name=line,
						opacity=1,
						mode="markers+lines",
						text = line,
						marker=dict(
							symbol="circle",
							size=6,
							line=dict(width=0,
								color='DarkSlateGrey'
								),
						),			
						line=dict(dash="solid", width=2), # dash = dot, dash, dashdot
						hovertemplate = hovertemplate1,						
					)
					'''does't work in offline mode
					def handle_click(trace, points, state):
						print(points.point_inds)
						logger.debug("points.point_inds")
						logger.debug("zzz")

					graph.on_click(handle_click)'''
				
				graphList.append(graph)
			
			#add vertical line
			today = date.today()
			graph = go.Scatter(
						x=[datetime.datetime(1904, today.month, today.day,0,0,0), datetime.datetime(1904, today.month, today.day,0,0,0)],
						y=[0, maxCumul],
						name="Now",
						opacity=1,
						mode="lines",
						line=dict(color="black", width=2), # dot, dash, dashdot
						text = line,
						marker=dict(
							symbol="circle",
							size=6,
							line=dict(width=0,
								color='DarkSlateGrey'
								),
						),	
						hoverinfo="none",
					)	
						
			graphList.append(graph)
						
			layout = go.Layout(
				title= layout_title ,
				autosize=True,
				xaxis_tickformat = '%-d-%b',
				yaxis_tickformat = yaxis_tickformat1,
				legend = dict(
					title="Year :",
					orientation="v",
					itemclick ="toggle",
					itemdoubleclick ="toggleothers"				
					),
				xaxis = dict(
					title = "Month",
					nticks =12
					#tickmode = 'linear',
					#type="start_date"
					),
				yaxis = dict(
					title = yaxis_title,
					tickmode=tickmode1,
					nticks =nticks1,
					dtick= dtick1
					),
				)	
			
			plot_div = offline.plot({"data": graphList,
				"layout": layout},
				include_plotlyjs=False,
				output_type='div')
				



				
	

	return plot_div
	
	

