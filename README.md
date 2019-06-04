# social-network-app

In order to start using this app as in my setup do the following:

### Requirements:
You may follow the instructions below to install what is required.

- Apache Kafka
- Apache Flink
- PostgreSQL database
- IntelliJ
- Docker
- Social network data may be found here: http://www.cs.albany.edu/~sigmod14contest/task.html

## Before beginning:
1. Install Docker on your local machine and then follow the instruction here: https://docs.confluent.io/current/quickstart/index.html
This will allow you to run Apache Kafka in Docker and have a nice UI were information about topics are available.
Once the UI is available you can create topics. To run this project you need to create the following topics:
    - comment
    - likes
    - post

    Note: if you want to delete or stop the entire Docker image running Kafka, you find the instruction in the quickstart. 
It may be helpful for example to restart with a clean Kafka installation or remove all the data that is stored in a topic.


Follow the following link to run a PostgreSQL database in Docker:
https://hackernoon.com/dont-install-postgres-docker-pull-postgres-bee20e200198

##App structure

The app is divided into modules:
- Model: contains datatypes for the data
- Common: contains classes that are used in multiple module
- Streamproducer: Reads data from CSV files and sends them to topics in Apache Kafka
- Activepoststatistics: Consumes data from the topics and makes some analysis with them.
- Recommendation: Contains very basic recommendation and may need some improvement
- Unusual activity detection: Detects if some of the streaming content is deviating from the standard deviation.


## Run the app
You can begin by importing each module (from the architecture) in IntelliJ by selecting the pom file.

The configuration should then set up automatically and you should be able to run each module
