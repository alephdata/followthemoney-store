FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -qq -y update \
    && apt-get -qq -y install python3-pip python3-dev python3-pil

RUN apt-get -qq -y install pkg-config libicu-dev
RUN pip install --no-binary=:pyicu: pyicu

# Environment variables used for the SQLAlchemy 2.0 migration
ENV SQLALCHEMY_WARN_20='1' \
    PYTHONWARNINGS='always'

COPY . /ftmstore
WORKDIR /ftmstore

RUN pip3 install --no-cache-dir followthemoney SQLAlchemy postgres pytest pyicu