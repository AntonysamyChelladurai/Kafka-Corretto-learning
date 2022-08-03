package io.digiwork.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerMultiTopic {
    private static final Logger log = LoggerFactory.getLogger(ProducerMultiTopic.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka Producer Demo");
        //create producer properties
        Properties prop=new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093,localhost:9094");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String > producer= new KafkaProducer<String, String>(prop);

        //producer Record
        ProducerRecord<String, String> producerRecod=
                new ProducerRecord<>("demo-kafka","hello corretto");

        // send data
        producer.send(producerRecod);

        // flush and close the producer
        producer.flush();
        producer.close();
    }
}
