import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.Consumer
import java.io._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import dispatch._, Defaults._
import sys.process._
import scalaj.http._
import scalaj.http.Http
import scala.sys.process._
import org.apache.commons._
import scala.util.control.NonFatal
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Time;
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.util.IntParam
import org.apache.spark.storage.StorageLevel

object KafkaHDFSZoomdata {

	def main(args: Array[String]): Unit = {
  	  if (args.length != 6) {
      System.err.println(s"""
        |Usage: KafkaHDFSSink <brokers> <topics> <destination-url> <offset_to_start_from> <outputformat> <time> <zoomdata_url> <username> <password>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is the topic to read from
        |  <destination-url> is the url prefix (eg:in hdfs) into which to save the fragments. Fragment names will be suffixed with the timestamp. The fragments are directories.(eg: hdfs:///temp/kafka_files/)  
        |  <offset_to_start_from> is the position from where the comsumer should start to receive data. Choose between: smallest and largest
		|  <output_format> is the file format to output the files to. Choose between: parquet, avro and json.
		|  <time> represents the number of seconds between succesive reads from the kafka topic
		|  <zoomdata_url> zoomdata upload url
		|  <username> zoomdata username
		|  <password> zoomdata password
		""".stripMargin)
      System.exit(1)
    } 
        val Array(brokers, topics, destinationUrl, offset, outputformat, time_string) = args

		//for http post
		val url = ("http://")
		val username = ("")
		val password = ("")

		//create spark streaming context
		val conf = new SparkConf().setMaster("local[2]").setAppName("kafka-hdfs-zoomdata")
		val ssc = new StreamingContext(conf, Seconds(time_string.toInt))

		//create spark stream from kafka	
		val kafkaParams = Map[String, Object](
		    "bootstrap.servers" -> brokers,
		    "key.deserializer" -> classOf[StringDeserializer],
		    "value.deserializer" -> classOf[StringDeserializer],
		    "group.id" -> "spark-playground-group",
		    "auto.offset.reset" -> offset,
		    "enable.auto.commit" -> (false: java.lang.Boolean)
		  )

	val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](Array(topics), kafkaParams))
		inputStream.foreachRDD( rdd => {
			//upload to zoom upload api http post scalaj
			rdd.foreach { record =>	
				val result = Http(url).postData(record.value().trim())
				.auth(username, password)
			  	.header("Content-Type", "application/json")
			  	.header("Charset", "UTF-8").asString
				}
			//write parquet to hdfs
			if(!rdd.partitions.isEmpty) {
				if(outputformat == "parquet") {
					try {
						val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
						import spark.implicits._
						val wordsDataFrame = rdd.map(w => Record(w.value())).toDF()
			        	wordsDataFrame.coalesce(1).write.mode(SaveMode.Append).parquet(destinationUrl);
			        		} catch {
									case NonFatal(t) => println("Waiting for more data")
									}
							}
			}
		})	
		ssc.start()
		ssc.awaitTermination()
	}
}


case class Record(word: String)

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
