Kafka Cryptocoin
================

Kafka producer for data collection from cryptocurrency exchanges

![Travis CI Status](https://travis-ci.org/blbradley/kafka-cryptocoin.svg)


Configuration
-------------

* `KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS`: Comma separated list of Kafka brokers. Port is required.


Kafka
-----

You must have a running Kafka broker. This project depends on 0.8.2.2 but is likely to
work with any version in the 0.8.2.x series. You can get it [here](http://kafka.apache.org/downloads.html).
Use the [Quick Start](http://kafka.apache.org/082/documentation.html#quickstart) to run
a broker locally for trying out this project.


Usage
-----

Start a Kafka broker or set your own.

    export KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS=localhost:9092
    sbt run

###Running the tests

    sbt test


Docker
------

###Run Docker image

Images for development and mainline versions are built and pushed to Docker Hub
when a pull request is merged. Merges into `develop` or `master` are published as
`develop` and `latest` respectively.

This downloads the latest development version. Start a Kafka broker or set your own.

    docker run -e KAFKA_CRYPTOCOIN_BOOTSTRAP_SERVERS=localhost:9092 coinsmith/kafka-cryptocoin:develop

###Build a Docker image

    sbt docker

Then, you can run it as specified above.
