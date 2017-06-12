# kafka-hdfs-zoomdata
Realtime hdfs sink and zoomdata post

This application takes 6 parameters:

BROKERS: List of Kafka Brokers with port number and splited with comma

DESTINATION URL: Location to save the files. Can be any supported filesystem location: FILE, HDFS, Bigstep Datalake etc.

KAFKA TOPIC OFFSET: Where should the broker start reading events.  Choose between: earliest and latest

OUTPUT FORMAT: What format should the data be written into. Available option: parquet

TIME: Number of seconds between succesive reads from the kafka topic.

The Zoomdata url, user and password must be configured in the app.

To compile use:  sbt package 

To submit this job use:

/opt/spark-2.1.1-bin-hadoop2.6/bin/spark-submit --master yarn target/scala-2.11/kafka-hdfs-zoomdata_2.11-1.0.jar
BROKER_FQDN_1:9092,BROKER_FQDN_2:9092,BROKER_FQDN_N:9092 TOPIC_NAME /path/to/write/files OFFSET FORMAT TIME 
