# syntax=docker/dockerfile:1
FROM python:3-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/local/nhl-data-projects

COPY ./requirements.txt /usr/local/nhl-data-projects/requirements.txt
RUN pip3 install -r requirements.txt

RUN pip3 install csvs-to-sqlite
RUN apt-get update && apt-get install make sqlite3

COPY .. /home/


