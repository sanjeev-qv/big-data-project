import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.collection.JavaConversions._


object KafkaConsumer {

  def main(args: Array[String]): Unit = {

    // CONSUMER
    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.servers", "127.0.0.1:9092")
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("group.id", "dummy-group")
    val consumer = new KafkaConsumer[String, String](consumerProperties)
    consumer.subscribe(Collections.singletonList("test"))

    while(true) {


      // READING
      val records = consumer.poll(100)
      for(record:ConsumerRecord[String, String] <- records) {
        println
        println("topic " + record.topic )
        println("value " + record.value )
      }
    }

//    producer.close()
    consumer.close()

  }
}