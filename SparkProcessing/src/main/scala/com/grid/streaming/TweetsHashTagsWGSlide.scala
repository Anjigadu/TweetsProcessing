package com.grid.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Count popular hash tags in tweets stream from Kafka within given window and offset
  * Uses experimental state functionality, timestamps are given from Spark batch
  */
object TweetsHashTagsWGSlide {
  val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Spark Streaming - By state - PopularHashTags")
    .set("spark.executor.memory", "1g")
    //parameter for debug!
    .set("spark.streaming.kafka.maxRatePerPartition","15")

  //conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

  val sc = new SparkContext(conf)
  val checkpointDir = "/twitter/checkpoint/114"
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
    // Create an array of arguments: zookeeper hostname/ip,consumer group, topic name, num of threa
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
    //println("new test 3")

    // Filter hash tags
    // output - strings with hash tags
    val hashTags = lines.flatMap(_._2.split("(\\s)|(\")"))filter(_.startsWith("#"))

    val slideMil = sliding.toInt*1000
    val windowMil = window.toLong*1000

    /**
      * Function of state updating
      * @param batchTime - the time of batch
      * @param key       - the tag
      * @param value     - pair of tag timestamp (shift time for tweet occurrence) and counts in shift interval of given tag
      * @param state     - start timestamp of current window, counts of current window,
      *                    timestamp of next shifting, count of next shifting,
      *                    list of (timestamp of shifting, counts of shifting)
      * @return None
      */
    def updateState(batchTime: Time, key: String, value: Option[Int],
                    state: State[(Time, Long, Time, Long, List[(Time, Long)], Long)])
    :Option[Boolean] = {

       if (test == null) {
         test = batchTime
         println("Set start!! "+test + ", sc_start "+sc.startTime)
       }

       val shiftTime = batchTime.minus(Milliseconds((batchTime.milliseconds - test.milliseconds) % slideMil))
       val prevWindowTime = shiftTime.minus(Seconds(window.toInt))

      //get previous state
      val (tw, cw, tt, ct, slides, cc) = state.getOption()
        .getOrElse(prevWindowTime, 0l, shiftTime, 0l, List(), 0l)

      var red = 0l
      var sum = cw
      var tempSum = value.getOrElse(0).toLong
      var list:List[(Time, Long)] = slides
      val updatedWindow = prevWindowTime

      //case when temp sliding will be added to list
      if (batchTime - tt >= Seconds(sliding.toInt)) {

        //need to find old values for deletion
        if (list.nonEmpty) {
          var newList: List[(Time, Long)] = List()
          for (item <- slides) {
            if (prevWindowTime > item._1) {
              red += item._2
            } else {
              newList = newList ++ List(item)
            }
          }

          list = newList
        }
        //case with old values that are not needed anymore
        if (tt < updatedWindow ) {
          sum -= red
        } else {
          //normal case
          list = list ++ List((tt, ct))
          sum += ct - red
        }
      } else {
        tempSum += ct
      }

      // updating the state of non-idle keys...
      // To call State.update(...) we need to check State.isTimingOut() == false,
      // else there will be NoSuchElementException("Cannot update the state that is timing out")
      if (state.isTimingOut())
        print("")
      //println(key + " key is timing out...will be removed.")
      else {
        state.update(updatedWindow, sum, shiftTime, tempSum, list, cc+value.getOrElse(0).toLong)
      }

      None
    }

    val stateSpec = StateSpec.function(updateState _).timeout(Seconds(window.toInt *2))

    //reduce by key and update state
    val wordCount = hashTags.map((_, 1)).reduceByKey(_+_).mapWithState(stateSpec)

      //filter not popular tags and if new window is ready then write it to hdfs
    wordCount.stateSnapshots()
    .transform((rrd, time) =>
    {
      if (test == null) {
        test = time
        println("Set start2: " + test+ ", sc_start "+sc.startTime)
      }
      val timestamp = if(!rrd.isEmpty()) rrd.map(r => r._2._1.milliseconds).max() else test.milliseconds
      rrd.filter(r => r._2._1.milliseconds >= timestamp - windowMil)
         .map { case (tag, composite) =>
        (
          if (composite._1.milliseconds != timestamp) {
            val additional = if (composite._3.milliseconds < timestamp + windowMil) composite._4 else 0l
            composite._2 + additional + (0l /: composite._5) ((sum: Long, x) => if (timestamp > x._1.milliseconds) sum - x._2 else sum)
          } else composite._2,
          (tag, timestamp + windowMil, //composite._1
            //information for debug
            composite._5 + " cc=" + composite._6 + ", tc=" + composite._4 + " tt=" + composite._3 + " + time=" + time +" wt="+composite._1 +" t="+timestamp//" sw = "+ shiftTime
          )
        )
      }.filter(_._1 >= numFilter.toInt).sortByKey(false)

    })
      .foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          val timestamp = rdd.map(r => r._2._2).max()
          val rdd1 = rdd
            .map(r => timestamp + " " + r._2._1 + " " + r._1 + " " + r._2._3+" "+ r._2._2+" "+(timestamp-test.milliseconds-slideMil))
          if (!rdd1.isEmpty()) {
            rdd1.saveAsTextFile("/twitter/state/" + (timestamp-test.milliseconds-slideMil))
          }
        }
      })

    // To make sure data is not deleted by the time we query it interactively
    ssc.remember(Minutes(3))

    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }
}
