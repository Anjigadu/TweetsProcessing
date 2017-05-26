package com.grid.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext};


/**
  * Created by apigul on 5/5/17.
  */
object TweetsHashTagsByStateSlide {
  val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Spark Streaming - By state - PopularHashTags")
    .set("spark.executor.memory", "1g")
    .set("spark.streaming.kafka.maxRatePerPartition","10")

  //conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

  val sc = new SparkContext(conf)
  val checkpointDir = "/twitter/checkpoint/114"
  var windowTime:Time = null
  var prevWindowTime:Time = null
  var slidingTime:Time = null
  var readyOutput = false

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")
    if (args.length < 9) {
      System.err.println(s"""
                            |Usage: KafkaStreamProcessing <zookeeper hostname/ip> <consumer group>
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
    //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val topicSet = topics.split(",").toSet

    // Setup Kafka params
    val kafkaParams = Map[String, String]("bootstrap.servers" -> "localhost:9092",
      "auto.offset.reset"-> offset,
      "group.id"->group,
      "zookeeper.connect"->zkQuorum)

    // Map value from the kafka message (k, v) pair
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    println("new test 3")

    // Filter hashtags
    val hashTags = lines.flatMap(_._2.split("(\\s)|(\")"))filter(_.startsWith("#"))

    //function to state update
    def updateState(batchTime: Time, key: String, value: Option[Int],
                    state: State[(Time, Long, Time, Long, List[(Time, Long)], Long)])
               :Option[(String, Long)] = {
      //initialization
      if (windowTime == null && slidingTime == null) {
        println("Start time: " + batchTime)
        windowTime = batchTime + Seconds(sliding.toInt)
        slidingTime = batchTime
        prevWindowTime = batchTime - Seconds(window.toInt - sliding.toInt)
      }
      //case when new sliding time will be created
      if (batchTime - slidingTime >= Seconds(sliding.toInt)) {
        println("Reset slidingTime: was - " + slidingTime + ", became -  " + (slidingTime + Seconds(sliding.toInt)))
        slidingTime = slidingTime + Seconds(sliding.toInt)
      }
      //case when new window time will be created
      if(batchTime - windowTime >= Seconds(batch.toInt + sliding.toInt)) {
        println("Reset windowTime: was - " + windowTime + ", became -  " + (windowTime + Seconds(sliding.toInt)))
        prevWindowTime += Seconds(sliding.toInt)
        windowTime += Seconds(sliding.toInt)
      }

      //get previous state
      val (tw, cw, tt, ct, slides, cc) = state.getOption()
                                              .getOrElse(prevWindowTime, 0l, slidingTime, 0l, List(), 0l)

      var red = 0l
      var sum = cw
      var tempSum = value.getOrElse(0).toLong
      var list:List[(Time, Long)] = slides

      //case when temp sliding will be added to list
      if (slidingTime - tt >= Seconds(sliding.toInt)) {
          //need to find old values for deletion
          //if (batchTime - prevWindowTime >= Seconds(window.toInt)) {
            //list.foreach(print)
            //print(key)
            if (list.nonEmpty) {
                var newList: List[(Time, Long)] = List()
                for (item <- slides) {
                  if (prevWindowTime > item._1) {
                    //prevWindowTime-item._1 <= Seconds(sliding.toInt) &&
                    red += item._2
                  } else {
                    newList = newList ++ List(item)
                  }
                }
                list = newList
            }
            list = list ++ List((tt, ct))
            sum += ct - red
            //case for first window
          //} else {
          //  sum += tempSum
          //}
        //case for accumulating sliding counts
      } else {
        //case for first window
        //if (batchTime - prevWindowTime < Seconds(window.toInt)) {
        //  sum += tempSum
        //}
        tempSum += ct
      }



      //println("Debug key: "+key+", batch time - " + batchTime + ", window time: " + windowTime)
      //update common sum and find last sliding sum
      //sum = value.getOrElse(0).toLong + cw - red

      // updating the state of non-idle keys...
      // To call State.update(...) we need to check State.isTimingOut() == false,
      // else there will be NoSuchElementException("Cannot update the state that is timing out")
      if (state.isTimingOut())
        print("")
        //println(key + " key is timing out...will be removed.")
      else {
        state.update(prevWindowTime, sum, slidingTime, tempSum, list, cc+value.getOrElse(0).toLong)
      }

      //Some((key, sum))
      None
    }

    val stateSpec = StateSpec.function(updateState _).timeout(Seconds(window.toInt + batch.toInt))

    //reduce by key and update state
    val wordCount = hashTags.map((_, 1)).reduceByKey(_+_).mapWithState(stateSpec)

    //filter not popular tags and if new window is ready then write it to hdfs
    .stateSnapshots()//wordCount
       //filter without consider not updated keys
      //.filter(_._2._2 >= numFilter.toInt)
      .transform((rrd, time) =>
    { println("Test -test: "+ time + ", window: " + prevWindowTime+", slidingTime " + slidingTime)
      rrd.filter( _ => //_._2._1 <= prevWindowTime &&
         //time - windowTime >= Seconds(sliding.toInt) &&
         time == prevWindowTime + Seconds(window.toInt) //time < slidingTime + Seconds(batch.toInt)
      ).map { case (tag, composite) =>
            (if (slidingTime - composite._3 >= Seconds(sliding.toInt))
                   composite._2 + composite._4+(0l/:composite._5)((sum:Long, x) => if (composite._1 > x._1) sum + x._2 else sum)
             else composite._2,
              (tag, composite._1+Seconds(window.toInt),
                //for debug
                composite._5+ " cc=" + composite._6 + ", tc="+ composite._4+" tt="+composite._3+" sw="+slidingTime + " time="+time + " ptime=" + prevWindowTime
              )
             ) } .filter(_._1 >= numFilter.toInt).sortByKey(false)
    })
      .foreachRDD(rdd => {
        val rdd1 = rdd.map(r => r._2._2 + " "+ r._2._1 + " " + r._1 + " "+ r._2._3)//
        //val topList = rdd.take(10)
        if (!rdd1.isEmpty()) {
          rdd1.saveAsTextFile("/twitter/state/" + System.currentTimeMillis())
        }
      })

    // To make sure data is not deleted by the time we query it interactively
    ssc.remember(Minutes(3))
    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }

}
