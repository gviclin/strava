from django.db import models
from django.utils import timezone


class User(models.Model):
	# Fields
	#django_user = models.ForeignKey('auth.User',on_delete=models.CASCADE)
	user_id = models.IntegerField(primary_key=True)
	firstname = models.CharField(max_length=50, null=True)
	lastname = models.CharField(max_length=50, null=True)
	access_token = models.CharField(max_length=50, null=True)
	weight = models.FloatField(null=True)
	sex = models.CharField(max_length=50, null=True)
	country = models.CharField(max_length=50, null=True)
	state = models.CharField(max_length=50, null=True)
	city = models.CharField(max_length=50, null=True)
	follower_count = models.IntegerField(null=True)
	friend_count = models.IntegerField(null=True)
	measurement_preference = models.CharField(max_length=50, null=True)
	ftp = models.IntegerField(null=True)
	
	updated_date = models.DateTimeField(
		default=timezone.now, null=True)
			
	strava_creation_date = models.DateTimeField(
		blank=True, null=True)
			
	first_activity_date = models.DateTimeField(
		blank=True, null=True, verbose_name="First activity ")
			
	last_activity_date = models.DateTimeField(
		blank=True, null=True, verbose_name="Last activity ")
		
	act_number = models.IntegerField(null=True, verbose_name="Number of activities ")
	
	year_run_objective = models.IntegerField(default=500, verbose_name="Year run target (km) ")
	year_ride_objective = models.IntegerField(default=500, verbose_name="Year ride target (km) ")
		
	# Metadata
	class Meta: 
		ordering = ['-user_id']
	
	def publish(self):
		self.published_date = timezone.now()
		self.save()

	def __str__(self):
		#String for representing the MyModelName object (in Admin site etc.).
		return self.firstname + " " + self.lastname
