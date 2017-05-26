package com.grid.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext};

/**
  * Count popular hash tags in tweets stream from Kafka within given window and offset
  * Uses experimental state functionality, timestamps are given from tweets
  * Need to be checked
  */
object TweetsHashTagsByShifts {
  val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Spark Streaming - By state - PopularHashTags")
    .set("spark.executor.memory", "1g")
    //parameter for debug
    .set("spark.streaming.kafka.maxRatePerPartition","200")

  //conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

  val sc = new SparkContext(conf)
  val checkpointDir = "/twitter/checkpoint/118"

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")
    if (args.length < 9) {
      System.err.println(s"""
                            Usage: KafkaStreamProcessing <zookeeper hostname/ip> <consumer group> <topicname> <num of threads> <batch> <window> <sliding> <offset reset> <filter by number of repetitions>
                            |  <window> window for calculation in seconds
         """.stripMargin)
      System.exit(1)
    }
    // Create an array of arguments: zookeeper hostname/ip,consumer group, topic name, num of threads
    val Array(zkQuorum, group, topics, numThreads, batch, window, sliding, offset, numFilter, startTime) = args
    val checkpointInterval = Seconds(sliding.toInt)

    // Set the Spark StreamingContext to create a DStream for every batch seconds
    val ssc = new StreamingContext(sc, Seconds(batch.toInt))
    val slideMil = sliding.toLong*1000
    val windowMil = window.toLong*1000
    val numberOfStreams = (windowMil - 1) / slideMil + 1
    val st = startTime.toLong

    // Map each topic to a thread
    val topicSet = topics.split(",").toSet

    // Setup Kafka params
    val kafkaParams = Map[String, String]("bootstrap.servers" -> "localhost:9092",
      "auto.offset.reset"-> offset,
      "group.id"->group,
      "zookeeper.connect"->zkQuorum)

    // Map value from the kafka message (k, v) pair
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    //println("new test 4")

    //get tags with timestamp of tweet creation
    //the output is: ((time of start shift period where tweet/tag is occurred, tag), counts of tags in shift interval)
    val tagsWithTime = lines.flatMap(_._2.split("timestamp\u001A")).filter(x=>x.contains("timestamp_ms"))
      .map(str => { val timeIndex = str.indexOf("timestamp_ms")
        val timestamp = str.substring(timeIndex+15, str.indexOf(",",timeIndex)-1).toLong
        val slidingTime = st + ((timestamp-st)/slideMil)*slideMil
        str.split("(\\s)|(\")").filter(s=>s.startsWith("#") && timestamp > st)
          .map(str =>((slidingTime, str), 1))}).flatMap(x=>x).reduceByKey((x,y)=>x+y)

    //save intermediate result for debug
    tagsWithTime
      .foreachRDD(rdd=> { val r = rdd.map(row => row._1._1 +" "+row._1._2 +" "+row._2)
        if (r != null && !r.isEmpty()) {
          r.saveAsTextFile("/twitter/temp/" + System.currentTimeMillis())
        }})

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
    def updateState(batchTime: Time, key: String, value: Option[(Long, Int)],
                    state: State[(Long, Long, Long, Long, List[(Long, Long)], Long)])
    :Option[Boolean] = {

      val (newTime, newVal) = value.getOrElse((st, 0))
      val shiftTime = st + ((newTime - st)/slideMil)*slideMil
      val prevWindowTime = shiftTime - windowMil

      //get previous state
      val (tw, cw, tt, ct, slides, cc) = state.getOption()
        .getOrElse(prevWindowTime, 0l, shiftTime, 0l, List(), 0l)

      var red = 0l
      var sum = cw
      var tempSum = newVal.toLong
      var list:List[(Long, Long)] = slides
      val updatedWindow = prevWindowTime

      //case when current shifting will be added to list and new current shifting will be started
      if (newTime - tt >= slideMil) {

        //need to find old values for deletion
        if (list.nonEmpty) {
          var newList: List[(Long, Long)] = List()
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
        state.update(updatedWindow, sum, shiftTime, tempSum, list, cc+newVal.toLong)
      }

      None
    }

    //be careful with timeout! if tweet timestamp window is bigger than batch window
    val stateSpec = StateSpec.function(updateState _).timeout(Seconds(window.toInt *2))

    var _i = 0
    while (_i < numberOfStreams) {
      val filteredStream = tagsWithTime.filter(rdd => (rdd._1._1 - st) % slideMil == 0)

      //update state
      val wordCount = filteredStream.map(x=>(x._1._2, (x._1._1, x._2)))
        .mapWithState(stateSpec)

      //get snapshot state, update data for not popular tags and write result to hdfs
      // (timestamp of window end, tag, count, debug info)
      wordCount.stateSnapshots()
        .transform(rrd =>
      {
        val timestamp = if(!rrd.isEmpty()) rrd.map(r => r._2._1).max() else 0l
        rrd.filter(r => r._2._1 >= timestamp - windowMil)
          .map { case (tag, composite) =>
            (
              if (composite._1 != timestamp) {
                val additional = if (composite._3 < timestamp + windowMil) composite._4 else 0l
                composite._2 + additional + (0l /: composite._5) ((sum: Long, x) => if (timestamp > x._1) sum - x._2 else sum)
              } else composite._2,
              (tag, timestamp + windowMil, //composite._1
                //for debug
                composite._5 + " cc=" + composite._6 + ", tc=" + composite._4 + " tt=" + composite._3 +" wt="+composite._1 +" t="+timestamp//" sw = "+ shiftTime
              )
            )
          }.filter(_._1 >= numFilter.toInt).sortByKey(false)

      })
        .foreachRDD(rdd => {
          if (!rdd.isEmpty()) {
            val timestamp = rdd.map(r => r._2._2).max()
            val rdd1 = rdd
              .map(r => (timestamp) + " " + r._2._1 + " " + r._1 + " " + r._2._3+" "+ r._2._2) // -sliding.toInt*1000      r._2._2-test.milliseconds
            if (!rdd1.isEmpty()) {
              rdd1.saveAsTextFile("/twitter/win/" + (timestamp))
            }
          }
        })

      _i += 1
    }

    // To make sure data is not deleted by the time we query it interactively
    ssc.remember(Minutes(3))
    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }
}
