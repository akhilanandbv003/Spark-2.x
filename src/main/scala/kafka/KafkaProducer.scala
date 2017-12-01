package kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/* Reads from a csv file and writes it to the topic
Uses Kafka Client Api
*/
object KafkaProducer extends App {

  val filePath = "/Users/avenk3/mygithub/Spark-2.x/src/main/resources/customers.csv" //args[0]
  val Products = scala.io.Source.fromFile(filePath).getLines().toArray
  val props = new Properties()
  val topic = "gziptopic" //args[1]

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "CSVProducer")
  props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")

  val kafkaProducer = new KafkaProducer[Nothing, String](props)

  Products.map { x =>
    val producerRecord = new ProducerRecord(topic, x)
    kafkaProducer.send(producerRecord)
  }

  kafkaProducer.close()
}



