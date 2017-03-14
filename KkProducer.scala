package org.apache.spark.examples.streaming;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

object KkProducer {
	def main(args: Array[String]) {
		if(args.length < 2){
			System.err.println("Usage: KkProducer <brokerlis> <topic>")
			System.exit(1)

		}
		val Array(brokers,topic)=args
		val props = new HashMap[String, Object]()

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      		"org.apache.kafka.common.serialization.StringSerializer")
    		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      		"org.apache.kafka.common.serialization.StringSerializer")

		val producer = new KafkaProducer[String, String](props)
		

		for (i <- 1 to 100)
			producer.send(new ProducerRecord[String, String](topic, Integer.toString(i),Integer.toString(i)));
		producer.close();
	}
}

