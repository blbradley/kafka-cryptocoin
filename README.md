Kafka Cryptocoin
================

Kafka producer for data collection from cryptocurrency exchanges

[![Build Status](https://travis-ci.org/blbradley/kafka-cryptocoin.svg?branch=develop)](https://travis-ci.org/blbradley/kafka-cryptocoin)


Configuration
-------------

### Environment Variables

* `KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS`: Comma separated list of Kafka brokers. Port is required.
* `KAFKA_CRYPTOCOIN_SCHEMA_REGISTRY_URL`: URL for Kafka Schema Registry
* `KAFKA_CRYPTOCOIN_PRODUCER_CONFIG`: Path to Kafka producer properties file (optional)

### application.conf

Main config is at `kafka.cryptocoin`.

* `exchanges`: List of enabled exchanges. Default is all supported exchanges.


Kafka
-----

You must have a running Kafka broker and Kafka Schema Registry.
These are part of [Confluent Platform 3.0](http://docs.confluent.io/3.0.0/index.html).
You may go to the [Quickstart](http://docs.confluent.io/3.0.0/quickstart.html)
to get them running locally for development.


Usage
-----

Start the the required services. Then, run:

    export KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS=localhost:9092
    export KAFKA_CRYPTOCOIN_SCHEMA_REGISTRY_URL=http://localhost:8081
    sbt run

### Running the tests

    sbt test


Docker
------

### Docker Compose

This repo includes a Compose file that will run the (development version of the) app and all services required for demo purposes.

    docker-compose up

Prepare for lots of output. The app should wait for the required services to start.


### Run Docker image

Images for development and mainline versions are built and pushed to [Docker Hub](https://hub.docker.com/r/coinsmith/kafka-cryptocoin)
when a pull request is merged. Merges into git branches `develop` or `master` are
images published on Docker Hub with tags `develop` and `latest` respectively.
Project releases are published with tags equal to the git release tag.


This downloads the latest development version and assumes your required services
are running locally.

    docker run --net=host \
    -e KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS=localhost:9092 \
    -e KAFKA_CRYPTOCOIN_SCHEMA_REGISTRY_URL=http://localhost:8081 \
    coinsmith/kafka-cryptocoin:develop


### Build a Docker image

    sbt docker

Then, you can run it as specified above. Images built from code will be tagged as the
current snapshot version.
