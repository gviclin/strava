import os
import json
import glob
import pandas as pd
import numpy as np
import cv2
import cufflinks as cf
import plotly.express as px
import pytz
from math import *

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as tick

import seaborn as sns
#from scipy import signalsignal
import datetime
import time
from pathlib import Path
from tabulate import tabulate
import pprint 
from loguru import logger

from stravaio import dir_stravadata
import stravaio 

class Statist():

	def __init__(self, logger):
		self.i = 5
		self.logger = logger
		self.strava_dir = dir_stravadata()
		
		
	def cleanDb(self, athlete_id):
		f_parquet = os.path.join(self.strava_dir, f"global_data_{athlete_id}.parquet")		
		if not os.path.exists(f_parquet):
			logger.debug("Local bd empty (not parquet file)")
		else:
			# Existing dataframe
			os.remove(f_parquet)

	def ComputeDatas(self,list_activities, athlete_id, startdate, enddate):
		""" ComputeDatas 

		Returns
		-------
		list including the dates of the first and the last activity and the number of activity
		"""
		#logger.debug("RetreiveFromDateInterval from <" + str(startdate) + "> to <" + str(enddate) + ">")
		#wait = input("PRESS ENTER TO CONTINUE.")
		#quit()

		#self.logger.debug(activities_dir)
		i = 0
		listAct = []
		for a in list_activities:
			file_data = a.to_dict()
			#logger.debug("file_data" + str(file_data))

			# Renvoie un datetime correspondant à la chaîne date_string, analysée conformément à format.
			#dt1 = datetime.datetime.strptime(file_data['start_date'], '%Y-%m-%dT%H:%M:%SZ')
			file_data['start_date'] = file_data['start_date'].replace(tzinfo=None)
			dt1 = file_data['start_date']
			if dt1 > startdate and dt1 < enddate:
				#self.logger.debug("open file number : " + str(i))
				#print(dt1)
				
				#filtering datas
				file_data.pop('athlete', None)		
				file_data.pop('map', None)	
				file_data.pop('achievement_count', None)	
				file_data.pop('athlete_count', None)	
				file_data.pop('elev_high', None)	
				file_data.pop('elev_low', None)	
				file_data.pop('external_id', None)	
				file_data.pop('photo_count', None)	
				file_data.pop('total_photo_count', None)	
				file_data.pop('upload_id', None)			
				file_data.pop('end_latlng', None)
				file_data.pop('kudos_count', None)
				file_data.pop('max_speed', None)		
				file_data.pop('max_watts', None)
				file_data.pop('start_date_local', None)		
				file_data.pop('start_latlng', None)
				file_data.pop('timezone', None)		
				#file_data.pop('total_elevation_gain', None)
				file_data.pop('weighted_average_watts', None)
				file_data.pop('average_speed', None)
				file_data.pop('average_watts', None)
				file_data.pop('comment_count', None)
				file_data.pop('gear_id', None)
				file_data.pop('has_kudoed', None)
				file_data.pop('kilojoules', None)
					
				df_temp = pd.DataFrame(file_data, index=[file_data["id"]])

				listAct.append(df_temp)
				i +=1
				'''if i >20:
					break'''
		
		#concatenate all dataframe
		newDf = pd.DataFrame()
		if listAct:		
			newDf = pd.concat(listAct)

			newDf['start_date'] =  pd.to_datetime(newDf['start_date'], format='%Y-%m-%dT%H:%M:%SZ')#, utc=True)
			#newDf.drop('start_date', axis=1,inplace=True)
			
			#logger.debug(newDf.info(verbose=True))
			
			#add year / month columns
			newDf['year'] = pd.DatetimeIndex(newDf['start_date']).year
			newDf['month'] = pd.DatetimeIndex(newDf['start_date']).month
#			newDf['week'] = pd.DatetimeIndex(newDf['start_date']).week
			newDf['week'] = pd.DatetimeIndex(newDf['start_date']).isocalendar().week
			
			#newDf['month'] = newDf['month'].apply(str)
			#newDf['year'] = newDf['year'].apply(str)
			#newDf['week'] = newDf['week'].apply(str)
			
			newDf.sort_values(by='start_date', inplace=True, ascending=True)
			
			newDf['distance'] = round(newDf['distance'] / 1000,3)
			
			#newDf = newDf.reindex(columns=sorted(newDf.columns))
			column_list = ['id', 'start_date','name', 'distance','elapsed_time','moving_time']
			list_col = (column_list + [a for a in newDf.columns if a not in column_list] ) 
			#print(str(list_col))
			newDf = newDf.reindex(columns=list_col)		
			newDf.set_index("id")
		'''else:
			logger.debug("No new activities") '''
			
		f_parquet = os.path.join(self.strava_dir, f"global_data_{athlete_id}.parquet")		
		
		if not os.path.exists(f_parquet):
			logger.debug("Local bd empty (not parquet file)")
			existingDf = newDf
			logger.debug("Local bd size changed from " + str(0) + " to " + str(len(existingDf)))
		else:
			# Existing dataframe
			existingDf = pd.read_parquet(f_parquet)	
			
			if not newDf.empty:			
				if not existingDf.empty:			
					oldSize = len(existingDf)
					
					# Remove datas belong to the given interval
					mask = (existingDf['start_date'] <= startdate) | (existingDf['start_date'] >= enddate)
					existingDf=existingDf.loc[mask]
					
					# Concatenate the 2 dataframes
					existingDf = pd.concat([existingDf, newDf]).drop_duplicates().reset_index(drop=True)			
					logger.debug("Local bd size changed from " + str(oldSize) + " to " + str(len(existingDf)))
				else:
					existingDf = newDf
			else:
				logger.debug("Local bd size not changed : " + str(len(existingDf)) + " elements")
		
		#existingDf['start_date'] = existingDf.start_date.dt.tz_convert(pytz.utc)
		#existingDf.tz_convert('UTC')
		
		#Store the dataframe		
		existingDf.to_parquet(f_parquet)
		
		#Store the dataframe in excell file
		#existingDf.to_excel(os.path.join(self.strava_dir, f"global_data_{athlete_id}.xlsx"))

		#Store the dataframe in html file
		#existingDf.to_html(os.path.join(self.strava_dir, f"global_data_{athlete_id}.html"))

		#return the date range of the datas
		
		ret = []
		
		try:
			ret = [	min(existingDf['start_date']),
					max(existingDf['start_date']),
					len(existingDf)]
				
		except:
			ret = [None, None, None]
		
		'''return [min(existingDf['start_date'] if not existingDf.empty anelse None),
				max(existingDf['start_date']  if not existingDf.empty else None),
				len(existingDf) if not existingDf.empty else None]'''
		return ret
				
					
	def Stat_dist_by(self, byWhat, athlete_id, listActivityType, listDataType):
		""" Compute_the_db 
		byWhat : "month" or "week"
		activityType : "Run", "Hike", "VirtualRide", "VirtualRun", "Walk","Ride"
		Returns
		-------
		Make stat by month or week and activity type
		"""
		df_data = pd.DataFrame()
		
		
		# Read global data file
		f_parquet = os.path.join(self.strava_dir, f"global_data_{athlete_id}.parquet")
		
		if not os.path.isfile(f_parquet):
			self.logger.debug("f_parquet file does not exist !!!" )
			return df_data
		
		df = pd.read_parquet(f_parquet)
		
		
		if not df.empty:		
			# Filter by type of activity
			filter = df["type"].isin(listActivityType)
			df_type = df[filter]		
		
			df_by = df_type.groupby(['year',byWhat]).sum()
			
			df_by = df_by[["distance","moving_time","total_elevation_gain"]]
			
			#df_by.drop('elapsed_time', axis=1,inplace=True)
			
			df_by["avg_speed"] = round(3600 * df_by["distance"] / df_by["moving_time"],1)
			df_by["avg_elev_by_10km"] = round(10 * df_by["total_elevation_gain"] / df_by["distance"],0)

			#print(tabulate(df, headers='keys', tablefmt='psql'))
			#pp.pprint(file_data)		
			#print(df.dtypes)

			dataType =""
			if len(listDataType):
				if listDataType[0] == "distance":
					dataType = "distance"
				elif listDataType[0] == "time":
					dataType = "moving_time"
				elif listDataType[0] == "elevation":
					dataType = "total_elevation_gain"
					
				self.logger.debug("dataType : " + dataType)
				
				df_data = df_by[[dataType]]
				
				df_data.reset_index(level="year", inplace=True)
				df_data.reset_index(level=byWhat, inplace=True)	

				# reshaped DataFrame organized by given index / column values.
				df_data = df_data.pivot(index='year', columns=byWhat, values=dataType)
				#df_data.reset_index(inplace=True)	
						
				df_data.fillna(0, inplace=True)
				
				print(str(df_data))
				#print(df_data.dtypes)
				print(df_data.index)
				
				#fill empty year
				listYear = list(df_data.index.values)
				for year in range(df_data.index.min() , df_data.index.max(), 1):
					if 	year not in listYear:	
						df_data.loc[year] = 0						
				df_data.sort_index(inplace=True, ascending=True)
					

						
				'''df=cf.datagen.lines(4)
				fig = df.iplot(asFigure=True, hline=[2,4], vline=['2015-02-10'])
				fig.show()'''
						
				#df_data.set_index("year")
				#df_data.drop('total', axis=1,inplace=True)
				df_data = df_data.T
				
				#fill empty month or week
				l = list(df_data.index.values)
				if byWhat=="month":
					begin = 1
					end = 12
				elif byWhat=="week":
					begin = 1
					end = 52
				else:
					begin = 1
					end = 1					
				for i in range(begin, end +1, 1):
					if 	i not in l:	
						df_data.loc[i] = 0						
				df_data.sort_index(inplace=True, ascending=True)				
				
				#print(df_data.info(verbose=True))					
				#print(tabulate(df_data, headers='keys', tablefmt='psql'))
				
				df_data.reset_index(inplace=True)
									
				#Create a column with the month or week string
				if byWhat=="month":
					df_data['month_str'] = df_data.apply(lambda row: datetime.date(1900, int(row["month"]), 1).strftime('%B'), axis=1)
					df_data.set_index("month_str",inplace=True)
				elif byWhat=="week":
					df_data['week_str'] = df_data.apply(lambda row: "Week " + str(row["week"]).zfill(2), axis=1)
					df_data.set_index("week_str",inplace=True)
					
				#add total row
				df_data.loc["Total"] = df_data.sum()
				
				df_data.drop(byWhat, inplace=True,axis=1)
				
				#round to an integer
				df_data = df_data.round(0).astype(int)
				
				df_data.columns.name = None
				df_data.index.name = None
				
				#print(tabulate(df_data, headers='keys', tablefmt='psql'))

				if dataType == "moving_time":
					listColumn = list(df_data)
					
					def convert_date(x):
						hour = floor(x /3600)
						remaing_sec = x - hour * 3600
						minute = floor(remaing_sec /60)
						return str(hour).zfill(2) + "h" + str(minute).zfill(2)
					
					for line in listColumn:						
						df_data[line] = df_data[line].apply(convert_date) 				
				
				#print(df_data)
				#print(df_data.info(verbose=True))					
				#print(tabulate(df_data, headers='keys', tablefmt='psql'))
				
				#os.remove(os.path.join(self.strava_dir, f"stat_{'_'.join(listActivityType)}_distance_{athlete_id}.xlsx"))
					
				#convert column year into string
				#df_data['year'] = df_data['year'].apply(str)
				
				'''df_data.to_parquet(os.path.join(self.strava_dir, f"stat_{'_'.join(byWhat)}_{'_'.join(listActivityType)}_{dataType}_{athlete_id}.parquet"))
				df_data.to_html(os.path.join(self.strava_dir, f"stat_{'_'.join(byWhat)}_{'_'.join(listActivityType)}_{dataType}_{athlete_id}.html"))
				df_data.to_excel(os.path.join(self.strava_dir, f"stat_{'_'.join(byWhat)}_{'_'.join(listActivityType)}_{dataType}_{athlete_id}.xlsx"))
				'''
				'''
				print("")
				print("type :",listActivityType)
				print(df_data.info(verbose=True))
				#print(df_data)
				print(tabulate(df_data, headers='keys', tablefmt='psql'))
				
				series_x = df_data["month_str"]
				df_data.drop("month", axis=1,inplace=True)
				list_month = list(df_data.columns)
				list_month.remove("month_str")
						
				fig = df_data.iplot(asFigure=True, xTitle="Month",
				yTitle="Distance", title="By month", x='month_str',y=list_month,
					mode = "lines+markers")
				fig.show()
				'''
				'''		
				fig, ax = plt.subplots()
				ax.set_title('Month statistics')
				ax.plot(
						series_x, df_data, 'x-'
						)
				ax.legend(list(df_data.columns.values), loc='upper right')
				fig.autofmt_xdate()
				plt.grid(True)			
				plt.show()'''
				
				#self.logger.debug("end of stat_by_year")
			else:
				self.logger.debug("no data list")
		return df_data	

	
	def Stat_dist_annual(self,athlete_id, listActivityType, dataType, dist_goal_list = [0]):
		""" Compute_the_db 
		listActivityType : "Run", "Hike", "VirtualRide", "VirtualRun", "Walk","Ride"
		Returns
		-------
		Make stat by month and activity type
		"""
		# Read global data file
		f_parquet = os.path.join(self.strava_dir, f"global_data_{athlete_id}.parquet")
		
		if not os.path.isfile(f_parquet):
			return pd.DataFrame()
			
		
		df = pd.read_parquet(f_parquet)
		
		# Filter by type of activity
		filter = df["type"].isin(listActivityType)
		df = df[filter]	
		
		#df = df[["year","month","start_date","distance","elapsed_time","moving_time","total_elevation_gain"]]
		df = df[["year","month","start_date",dataType, "id"]]
		
		df.id = df.id.astype(int)
		
		df.sort_values(by='start_date', inplace=True, ascending=True)
		
		df.set_index("start_date",inplace=True, drop=False)
		
		df['cumul'] = df.groupby(df.index.year)[dataType].cumsum()
		
		df["start_date"] = df.apply(lambda row:  row["start_date"].replace(year = 1904), axis=1)
		
		df = df[["year","month","start_date","cumul",dataType,"id"]]
		
		#distance goal
		if dataType=="distance":
			for goal in dist_goal_list:
				strGoal =   f"{goal} km"
				dt_goal = pd.date_range(datetime.datetime(1904, 1, 1,0,0,0), periods=365, freq='D')
				ts = pd.Series(range(len(dt_goal)), index=dt_goal)
				frame = { 'cumul': ts } 
				result = pd.DataFrame(frame) 
				result["cumul"] = result.apply(lambda row:  goal * row["cumul"] / 364, axis=1)
				#result["distance"] = result.apply(lambda row:  goal / 364, axis=1)
				result.reset_index(inplace=True)
				result['month'] = pd.DatetimeIndex(result['index']).month		
				result['month'] = result['month'].apply(str)
				result['year'] = strGoal
				result['id'] = 0		
				result.rename({'index': 'start_date'}, axis=1, inplace=True)
				result.set_index(keys="start_date", inplace=True, drop=False)			
				df = pd.concat([df,result])
		'''
		print("")
		print("type :",listActivityType)
		print(df.info(verbose=True))
		print(df)'''
		
		#print(tabulate(df, headers='keys', tablefmt='psql'))
		#df.to_html(os.path.join(self.strava_dir, f"temp.html"))
		
		# Y axis : distance beetween 2 ticks
		'''dtick1 = 100 if "Run" in listActivityType else 1000
		
		fig = px.line(
			df,
			 x="start_date",
			  y="cumul",
			  line_group="year",
			  color="year",
			  custom_data=["id","year","distance"],
			  #hover_name=df["distance"]
			  #hover_data=["month", "cumul"]
			  )
		#All traces	  
		fig.update_traces(
			mode="markers+lines",
			marker=dict(
				symbol="circle",
				size=6,
				line=dict(width=0,
					color='DarkSlateGrey'
					)
				),			
			line=dict(dash="solid", width=2), # dot, dash, dashdot
			text = df["year"],
			hovertemplate = '%{x}<br> %{y:.1f} kms<br>activity %{customdata[2]:.1f} kms<br>link %{customdata[0]}'			
			)
			
		# Only goal trace
		for goal in dist_goal_list:
			fig.update_traces(
				selector=dict(name=f"{goal} km"),
				mode="lines",
				line=dict(dash="dashdot", width=3), # dot, dash, dashdot
				hovertemplate = '%{x}<br>%{y:.1f} kms'
				)
			
		fig.update_layout(
			title=f"Annual {listActivityType[0]} Statistics",
			#hovermode="x unified",
			xaxis_tickformat = '%-d-%b',
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
				title = "Cumul Km",
				nticks =20,
				dtick= dtick1
			)
		)	
		
		# create our callback function
		def update_point(trace, points, selector):
			print("toto")
			c = list(scatter.marker.color)
			s = list(scatter.marker.size)
			for i in points.point_inds:
				c[i] = '#bae2be'
				s[i] = 20
				with f.batch_update():
					scatter.marker.color = c
					scatter.marker.size = s

		scatter = fig.data[0]

		#scatter.on_click(update_point)
		
		#df.to_excel(os.path.join(self.strava_dir, f"stat_{'_'.join(listActivityType)}_annual_distance_{athlete_id}.xlsx"))

		#fig.show()
		'''
		
		'''fig = df.iplot(asFigure=True, xTitle="Month",
		yTitle="Distance", title="Annual statistics", x="dt",y="cumul",
		mode = "lines")
		fig.show()'''
		
		'''filter = df["year"] == "2020"
		df = df[filter]	'''
		

			
		return df
		
