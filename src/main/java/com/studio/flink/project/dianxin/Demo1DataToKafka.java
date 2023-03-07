package com.studio.flink.project.dianxin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class Demo1DataToKafka {
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        List<String> lines = Files.readAllLines(Paths.get("src/main/java/com/studio/flink/project/dianxin/dianxin_data"), StandardCharsets.UTF_8);
        for (String line : lines) {
            ProducerRecord<String, String> record = new ProducerRecord<>("dianxin", line);
            kafkaProducer.send(record);
            kafkaProducer.flush();
            Thread.sleep(1000);
        }
        kafkaProducer.close();
    }
}
