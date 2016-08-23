Kafka Cryptocoin
================

Kafka producer for data collection from cryptocurrency exchanges

![Travis CI Status](https://travis-ci.org/blbradley/kafka-cryptocoin.svg)


Configuration
-------------

* `KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS`: Comma separated list of Kafka brokers. Port is required.
* `KAFKA_CRYPTOCOIN_SCHEMA_REGISTRY`: URL for Kafka Schema Registry


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

###Running the tests

    sbt test


Docker
------

###Run Docker image

Images for development and mainline versions are built and pushed to Docker Hub
when a pull request is merged. Merges into `develop` or `master` are published as
`develop` and `latest` respectively.

This downloads the latest development version.

    docker run \
    -e KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS=localhost:9092 \
    -e KAFKA_CRYPTOCOIN_SCHEMA_REGISTRY_URL=http://localhost:8081 \
    coinsmith/kafka-cryptocoin:develop


###Build a Docker image

    sbt docker

Then, you can run it as specified above.
