# kafka_app

## Description
Welcome to kafka streams application. This application allows clients to get the users whic fit the criteria as desired by the user. Moreover, the users are shown with extremely low latency and applies the queries on the real time data. The framework used is Kafka and SpringBoot.

The application starts with the client giving the query. When that endpoint is hit, a new instance of kafka streams is created which applies the necessary filters as per the query and sends the events satisfying our query to aggregate operation. During the aggregation, streams make the user name as the key and then aggregates the data of the user with the required atttributes. This transformed event will now be pushed to an output topic. 
Simultaneoudly, a consumer is also running which is consuming the events from the output topic. This consumer then applies filters necessary for our query and pushes the name along with their latest timestamp in the hashmap.
There is also a scheduler running which is printing the contents of the Hashmap and hence gives the resutl of the query

## How to Run the project
You can clone the github repo into your systme and load the gradle build. After that you need to have a kafka broker and a zookeeper installed and running on the server. Once this is done you can just start the application and hit the endpoints required for your use case. To test at random events there is also an event generator class which helps in populating the topic so that user can get the idea of how the application is working.
