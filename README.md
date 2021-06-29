# Analysis of public transport traffic in Warsaw with the use of stream processing

## Data flow
ZTM -> NiFi -> Kafka -> Spark -> Kafka -> MongoDB

## Run description 
### Set up docker containers
Project can be run locally and in cloud environment. 
To run project locally you have to use docker-compose with configuration written in file: 
https://github.com/m-qlas/public-transport-traffic-analisys/blob/main/docker-compose/nifi-kafka-single-mongo.yml.       
Docker containers with _Zookeper, Kafka, Kafka-Connect, Nifi, MongoDB_ will be started. 
### Nifi 
After docker container with Nifi have been started you have to import and run templates from:
https://github.com/m-qlas/public-transport-traffic-analisys/tree/main/Nifi_templates

### Zookepper
Additional configuration don't have to be done after container start. 

### Kafka
Additional configuration don't have to be done after container start. 

### Kafka-Connect
Configuration files from:
https://github.com/m-qlas/public-transport-traffic-analisys/tree/main/Kafka_connect_confs
have to be added at the path: `/etc/kafka` inside container
Next connection with MongoDB can be started with command: 
`connect-standalone /etc/kafka/connect-standalone.properties /etc/kafka/connect-mongo.properties`

### MongoDB
Additional configuration don't have to be done after container start. 

### Spark
You have to activate python environment with pyspark library on your machine. 
In first step dataframe with schedule have to be created, it can be done by running script written in file:
https://github.com/m-qlas/public-transport-traffic-analisys/blob/main/ImportSchedule.py
After download has been finished you can start stream processinng for trams and buses with files:
https://github.com/m-qlas/public-transport-traffic-analisys/blob/main/BusStream.py
https://github.com/m-qlas/public-transport-traffic-analisys/blob/main/TramStream.py

After everything is properly configured and stream is started, data will be written to schemas _tram_ and _bus_ in MongoDB.
Example querries to recieve can be found in:
https://github.com/m-qlas/public-transport-traffic-analisys/blob/main/mongo_commands.js
