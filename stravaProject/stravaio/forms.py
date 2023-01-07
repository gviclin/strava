from django import forms

from .models import User

class PostSettings(forms.ModelForm):

    class Meta:
        model = User
        fields = ('year_run_objective', 'year_ride_objective',)
