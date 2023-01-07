from django import forms
from django.forms import widgets , DateTimeField

from .models import User

class PostSettings(forms.ModelForm):
	class Meta:
		model = User
		fields = (	'year_run_objective', 
					'year_ride_objective',
					'first_activity_date',
					'last_activity_date',
					'act_number')

		widgets = {
			'first_activity_date': 	forms.TextInput(attrs={'disabled': True}),
			'last_activity_date': 	forms.TextInput(attrs={'disabled': True}),
			'act_number': 			forms.TextInput(attrs={'disabled': True}),
		}
		'''widgets = {
			'first_activity_date': widgets.Textarea(attrs={'cols': 80, 'rows': 20}),
		}'''
		'''help_texts = {
			'first_activity_date': ('first_activity_date'),
		}'''
