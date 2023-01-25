# syntax=docker/dockerfile:1
FROM  python:3-alpine
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /home
COPY .. /home/
RUN pip3 install -r requirements.txt

RUN apk add --no-cache bash
RUN apk add --no-cache sqlite
RUN apk add --no-cache make

# RUN apk add --no-cache --update \
#     python3 python3-dev gcc \
#     gfortran musl-dev g++ \
#     libffi-dev openssl-dev \
#     libxml2 libxml2-dev \
#     libxslt libxslt-dev \
#     libjpeg-turbo-dev zlib-dev

# RUN pip install --upgrade pip

