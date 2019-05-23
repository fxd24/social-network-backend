# social-network-app

In order to start using this app as in my setup do the following:

## Requirements:
IntelliJ
Docker
Social network data may be found here:  http://www.cs.albany.edu/~sigmod14contest/task.html

## Before beginning:
Follow the instruction here: https://docs.confluent.io/current/quickstart/index.html
This will allow you to run Apache Kafka in Docker and have a nice UI were information about topics are available.
Once the UI is available you can create topics. To run this project you need to create the following topics:
- comment
- likes
- post


Follow the following link to run a postgres database in docker:
https://hackernoon.com/dont-install-postgres-docker-pull-postgres-bee20e200198

##App structure

The app is divided into modules:
- Model: contains Classes for the data
- Common: contains classes that are used in multiple module
- Streamproducer: Reads data from CSV files and sends them to topics in Apache Kafka
- Activepoststatistics: Consumes data from the topics and makes some analysis with them.

