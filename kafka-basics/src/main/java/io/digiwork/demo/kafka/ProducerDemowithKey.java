package io.digiwork.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithKey {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemowithKey.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("Kafka Producer Demo");
        //create producer properties
        Properties prop=new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());



        //create the producer
        KafkaProducer<String,String > producer= new KafkaProducer<String, String>(prop);


        //producer Record
        for(int i=0;i<=10;i++) {
            String topic="demo-kafka";
            String key="id_"+i;
            String value="Kafka with ID :"+i*20;
            ProducerRecord<String, String> producerRecod =

                    new ProducerRecord<>(topic,key, value);
            // send data
            producer.send(producerRecod, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executes every time record is successfully sent or exception thrown
                    if (e == null) {
                        log.info("\n Received new metadata /// ///  /// \n" +
                                " Topic     : " + metadata.topic() +"\n"+
                                " Key       : " + producerRecod.key()+"\n"+
                                " Partition : " + metadata.partition() +"\n"+
                                " Offset    : " + metadata.offset() +"\n"+
                                " TimeStamp : " + metadata.timestamp()+"\n");
                    } else {
                        log.error("Error while Producing : ", e);
                    }
                }
            });
            Thread.sleep(1000);
        }
        // flush and close the producer
        producer.flush();
        producer.close();
    }
}
