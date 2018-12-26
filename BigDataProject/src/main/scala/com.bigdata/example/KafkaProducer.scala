package com.bigdata.example

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.io.Source

object KafkaProducer {

  def main(args: Array[String]): Unit = {

    // PRODUCER
    val producerProperties = new Properties()
    producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](producerProperties)



    val source = Source.fromFile("/Users/sanjeev/BigDataProject/src/main/scala/com.bigdata/example/abc.csv")
    for (line <- source.getLines()){

      val record = new ProducerRecord[String, String]("test", line)

//      // fire and forget
//      producer.send(record)

      //    // send returns future
      //    val f = producer.send(record)

      // send with callback
      producer.send(record, (m:RecordMetadata, e:Exception) => {
        println
        println("producer callback:")
        println("checksum " + m.checksum())
        println("offset " + m.offset())
        println("partition " + m.partition())
        println("serialized key size " + m.serializedKeySize())
        println("serialized value size " + m.serializedValueSize())
        println("timestamp " + m.timestamp())
        println("topic " + m.topic())
        println("exception" + e)
      })

    }
    source.close()



  }

  }
