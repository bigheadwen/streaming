/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object HwenTest {
  case class Record(key:String,value1:String,value2:String)extends Serializable{
     override def toString:String="%s\t%s\t%s".format(key,value1,value2)
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: HwenTest <brokers> <topics> <hdfs>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |  <hdfs> is hdfs path like hdfs://hostname:9000/tmp/test
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics, hdfspath) = args
    val sparkConf = new SparkConf().setAppName("HwenTest")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val sqlContext=new HiveContext(sc)
    import sqlContext.implicits._

    // Create direct kafka stream with brokers and topics
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
//    val record=lines.filter(x=> !x.isEmpty()).map{x =>
    val record=lines.map(_.split(" ")).filter(_.length==3).map{x =>
      val key=x(0)
      val v1=x(1)
      val v2=x(2)
      val r1=new Record(
      key,v1,v2
      )
      r1	
    }
    record.foreachRDD(x  => {
       x.toDF().write.mode(SaveMode.Append).saveAsTable("pokes3")

      //x.toDF().registerTempTable("tb_record")
      //val recordDF=sqlContext.sql("insert overwrite table pokes3 select * from tb_record")
      //val recordDF=sqlContext.sql("insert overwrite table pokes3 select * from tb_record")
      val record1DF=sqlContext.sql("select * from pokes3")
      record1DF.show()
    //  if(!x.isEmpty()) {
    //  x.foreachPartition{res => {
    //    res.foreach{
    //	    r:Record =>  println("hello",r.value2)   
    //    } 
    //  }
    //  }
    //  }

    })

   // lines.Map(_.split("\n")).foreachRDD(line=>println("hello",line))
    println("Result of 'hwen': ")
    //val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    val suffix="001"
//    wordCounts.print()
//    wordCounts.saveAsTextFile(hdfspath)
//    wordCounts.saveAsTextFiles(hdfspath,suffix)
//    wordCounts.saveAsHadoopFiles(hdfspath)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
