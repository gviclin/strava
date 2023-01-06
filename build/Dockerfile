# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt

#copy project code
#COPY ./stravaProject /app 

WORKDIR /app
EXPOSE 8000

#CMD ["django-admin", "startproject", "stravaProject"]
#CMD ["python3", "stravaProject/manage.py", "startapp", "stravaApp"]

#ENTRYPOINT ["python3"] 
CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]
