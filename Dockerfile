# syntax=docker/dockerfile:1
FROM python:3.11.2-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/local/nhl-data-projects

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get -y install make wget time git build-essential

RUN wget https://www.sqlite.org/2022/sqlite-autoconf-3400100.tar.gz -P /tmp/sqlite/
RUN cd /tmp/sqlite && \
    tar xvfz sqlite-autoconf-3400100.tar.gz && \
    cd sqlite-autoconf-3400100 && \
    ./configure --prefix=/usr/local && \
    make && \
    make install

COPY ./requirements.txt /usr/local/nhl-data-projects/requirements.txt
RUN pip3 install -r requirements.txt

COPY .. /usr/local/nhl-data-projects/
# RUN datasette install datasette-publish-fly
# EXPOSE 8001

