CHANGELOG
=========

v0.0.3
------

* exchanges can be disabled via configuration


v0.0.2
------

* reimplementation using Akka Streams for polling and Actors for streaming
* removal of [XChange](https://github.com/timmolter/XChange) usage due to inconsistent streaming implementations
* updated to new Kafka Producer API
* updated to Kafka 0.10.0.0
* Avro data format for Kafka topics
* consistent Avro schemas for similar data elements across different exchanges
* topic per exchange and data type


v0.0.1
------

* Added OKCoin polling
* Added Polling supports order book data
* Added Bitstamp streaming
* Streaming collects order book snapshots, depth updates, and trades


v0.0.0
------

* Initial release
* Bitstamp and BitFinex support
* Polls ticker data every 30 seconds
