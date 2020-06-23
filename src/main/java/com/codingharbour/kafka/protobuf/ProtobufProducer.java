package com.codingharbour.kafka.protobuf;

import com.codingharbour.protobuf.SimpleMessageProtos.SimpleMessage;
import com.google.protobuf.GeneratedMessageV3;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class ProtobufProducer {

    public static void main(String[] args) {
        ProtobufProducer protobufProducer = new ProtobufProducer();
        protobufProducer.writeMessage();
    }

    public void writeMessage() {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        properties.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        Producer<String, GeneratedMessageV3> producer = new KafkaProducer<>(properties);

        //prepare the message
        SimpleMessage simpleMessage =
                SimpleMessage.newBuilder()
                        .setContent("Hello world")
                        .setDateTime(Instant.now().toString())
                        .build();

        System.out.println(simpleMessage);

        //prepare the kafka record
        ProducerRecord<String, GeneratedMessageV3> record = new ProducerRecord<>("protobuf-topic", null, simpleMessage);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }

}
