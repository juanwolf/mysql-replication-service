# Dockerfile for ElasticSearch and Kibana stack on Fedora base

# Help:
# Default command: docker run -d -p 5601:5601 -p 9200:9200 juanwolf/docker-ekauto /ek_start.sh
# Default command will start EK within a docker
# To send data to ek, stream to TCP port 3333
# Example: echo 'test data' | nc HOST 3333. Host is the IP of the docker host
# To login to bash: docker run -t -i juanwolf/docker-ekauto /bin/bash


FROM centos:latest
MAINTAINER Jean-Loup Adde

# Initial update
RUN yum update

RUN yum install wget -y

RUN yum install -y java-1.7.0-openjdk

# This is to install add-apt-repository utility. All commands have to be non interactive with -y option
# RUN DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common

# Elasticsearch installation
# Start Elasticsearch by /elasticsearch/bin/elasticsearch. This will run on port 9200.
RUN ELASTIC_VERSION=1.6.0 && \
    wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-$ELASTIC_VERSION.tar.gz && \
	tar xf elasticsearch-$ELASTIC_VERSION.tar.gz && \
	rm elasticsearch-$ELASTIC_VERSION.tar.gz && \
	mv elasticsearch-$ELASTIC_VERSION elasticsearch

# Kibana installation
RUN KIBANA_VERSION=4.1.0 && \
    KIBANA_FILENAME=kibana-$KIBANA_VERSION-linux-x64 && \
    wget https://download.elastic.co/kibana/kibana/$KIBANA_FILENAME.tar.gz && \
	tar xf $KIBANA_FILENAME.tar.gz && \
	rm $KIBANA_FILENAME.tar.gz && \
	mv $KIBANA_FILENAME  kibana

# Install curl utility just for testing
RUN yum update

# Create a start bash script
RUN touch ek_start.sh && \
	echo '#!/bin/bash' >> ek_start.sh && \
	echo '/elasticsearch/bin/elasticsearch &' >> ek_start.sh && \
	echo '/kibana/bin/kibana &' >> ek_start.sh && \
	chmod 777 ek_start.sh

# 5601=kibana, 9200=elasticsearch
EXPOSE 5601 9200