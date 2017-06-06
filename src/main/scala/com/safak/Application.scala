package com.safak

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  * <brokers> is a list of one or more Kafka brokers
  * <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  * $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  * topic1,topic2
  */
object DataConsumer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf()
      .setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // Create a new stream which can decode byte arrays.
    val messageStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topicsSet)

    messageStream.foreachRDD(rdd => {

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      if (!rdd.isEmpty()) {
        val df = sqlContext.read.json(rdd.map(_._2))
        println(df.count)
      }
    });

    ssc.start()
    ssc.awaitTermination()
  }

  /** Lazily instantiated singleton instance of SQLContext */
  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) instance = new SQLContext(sparkContext)
      instance
    }
  }
}
