# Progetto-Big-Data2


##Project report

details on project implementation and performance can be found in the pdf inside the folder `/report`

## Clone the repository

- `git clone git@github.com:naddi96/progetto-Big-Data.git`


## Start Kafka & create topic

- dowload kafka `https://kafka.apache.org/downloads`

- go inside `cd containerized-kafka\'

- exucute `./launchEnvironment.sh`

- create topic output-stream `$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic input-stream`

- create topic input-stream `$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic output-stream`

##Start Consumer

- import the `/consume` folder inside intelij/eclise and run the main class or siply use `mvn exec:java -Dexec.mainClass="ConsumerLauncher"`



##Start Processor

- import the `/codice-flink` or `/codice-kafka` folder inside intelij/eclise and run the main class or siply use maven from comand line:
	-- for query1 in flink `mvn exec:java -Dexec.mainClass="naddi.sadb.progetto.query1.Query1"`
	-- for query2 in flink `mvn exec:java -Dexec.mainClass="naddi.sadb.progetto.query2.Query2"`
	-- for query1 in kafka-streams `mvn exec:java -Dexec.mainClass="stream.Query1"`



##Start Producer

- import the `/producer` folder inside intelij/eclise and run the main class or siply use `mvn exec:java -Dexec.mainClass="BussDelayProducerLauncher"`




##View of the processing pipeline


![processing infrastructure](https://github.com/naddi96/Progetto-Big-Data2/blob/master/report/architettura.png?raw=true)``
