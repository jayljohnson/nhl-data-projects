# syntax=docker/dockerfile:1
FROM python:3-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/local/nhl-data-projects

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get -y install make sqlite3 time git

COPY ./requirements.txt /usr/local/nhl-data-projects/requirements.txt
RUN pip3 install -r requirements.txt

COPY .. /usr/local/nhl-data-projects/
# RUN datasette install datasette-publish-fly
# EXPOSE 8001

