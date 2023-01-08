# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt

RUN apt-get update && apt-get -y install libgl1

COPY requirements2.txt /code/
RUN pip install -r requirements2.txt

#RUN groupadd -r user && useradd -r -g user user
#USER user

#copy project code
#COPY ./stravaProject /app 

WORKDIR /app
EXPOSE 8000

#CMD ["django-admin", "startproject", "stravaProject"]
#CMD ["python3", "stravaProject/manage.py", "startapp", "stravaApp"]

#ENTRYPOINT ["python3"] 
#CMD ["gunicorn", "--bind", "0.0.0.0:8000", "stravaProject.wsgi"]
#CMD ["gunicorn", "--bind", "0.0.0.0:8000", "stravaProject.wsgi", "--log-level=debug"]
