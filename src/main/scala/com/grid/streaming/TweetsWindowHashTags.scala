package com.grid.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext};

/**
  * Count popular hash tags in tweets stream from Kafka within given window and offset
  * Uses build-in functionality, timestamps are given from Spark batch
  */
object TweetsWindowHashTags {

  val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Spark Streaming - Window var - PopularHashTags")
    .set("spark.executor.memory", "1g")
    //parameter for debug!
    .set("spark.streaming.kafka.maxRatePerPartition","15")

  //conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

  val sc = new SparkContext(conf)
  val checkpointDir = "/twitter/checkpoint/113"
  var test:Time = null

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")
    if (args.length < 9) {
      System.err.println(s"""
                            |Usage: KafkaStreamProcessing <zookeeper hostname/ip> <consumer group> <topicname> <num of threads> <batch> <window> <sliding> <offset reset> <filter by number of repetitions>
                            |  <window> window for calculation in seconds
           """.stripMargin)
      System.exit(1)
    }
    // Create an array of arguments: zookeeper hostname/ip,consumer group, topic name, num of threads, batch, window, sliding
    val Array(zkQuorum, group, topics, numThreads, batch, window, sliding, offset, numFilter) = args
    val checkpointInterval = Seconds(sliding.toInt)

    // Set the Spark StreamingContext to create a DStream for every batch seconds
    val ssc = new StreamingContext(sc, Seconds(batch.toInt))

    // Map each topic to a thread
    val topicSet = topics.split(",").toSet

    // Setup Kafka params
    val kafkaParams = Map[String, String]("bootstrap.servers" -> "localhost:9092",
      "auto.offset.reset"-> offset,
      "group.id"->group,
      "zookeeper.connect"->zkQuorum)

    // Map value from the kafka message (k, v) pair
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    //println("new test 2")

    // Filter hash tags
    // output - strings with hash tags
    val hashTags = lines.flatMap(_._2.split("(\\s)|(\")"))filter(_.startsWith("#"))

    //count hash tags for given window and offset
    //output - (hash tag, counts)
    val rawCounts = hashTags.map((_, 1))
      .reduceByKeyAndWindow((x: Int, y: Int) => x+y, //reduce condition
        (x: Int, y: Int) => x-y, //incremental inverting
        Seconds(window.toInt),   // window interval
        Seconds(sliding.toInt),  //sliding interval
        2,                       // partitions
        (x: (String, Int)) => x._2 != 0) //filtering of 0 counts after inverting

    // Checkpoint the dstream. (If the stream is not checkpointed, the performance will deteriorate significantly over time and eventually crash.)
    rawCounts.checkpoint(checkpointInterval)

    //filter by popularity, sort
    //output - (counts, hash tags)
    val topCounts = rawCounts.filter(_._2 > numFilter.toInt).map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    //add timestamp
    // output - (timestamp, counts, tag, offset from start)
    val topCountsWithTime = topCounts.transform((rdd, time) => {
      println("Top count generated at: "+ time)
      if (test == null) {
        test = time
      }
      rdd.map(a => time.milliseconds + " " + a._1 + " " + a._2+" "+(time.milliseconds-test.milliseconds))})

    // Print popular hash tags
    topCountsWithTime.foreachRDD(rdd => {
      val time = rdd.take(1).map(a => a.split(" ")(0)).max
      if (!rdd.isEmpty()) {
        rdd.saveAsTextFile("/twitter/red/" + time);
      }
    })

   // To make sure data is not deleted by the time we query it interactively
    ssc.remember(Minutes(3))

    ssc.checkpoint(checkpointDir)

    ssc.start()
    ssc.awaitTermination()
  }
}
