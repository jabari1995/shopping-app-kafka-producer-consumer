package com.shopping.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shopping.kafka.model.Product;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * The ProductProducer class is responsible for producing messages to a Kafka topic.
 * It sends a JSON representation of a Product object to the specified Kafka topic.
 */
public class ProductProducer {
    private static final Logger log = LoggerFactory.getLogger(ProductProducer.class);
    private static final String PRODUCT_TOPIC = "products";

    /**
     * This method is the entry point of the application.
     * It sends a JSON representation of a Product object to a Kafka topic.
     *
     * @param args an array of command-line arguments
     * @throws InterruptedException if the thread is interrupted while waiting for the producer to send the message
     * @throws ExecutionException   if the producer encounters an error while sending the message
     * @throws JsonProcessingException if there is an error during JSON serialization of the Product object
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException, JsonProcessingException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Product product = new Product(3, "Poloshirt, Kurzarm (2er Pack)", 25.99, "https://image01.bonprix.de/assets/275x385/1684855332/23077857-j6aULXSb.webp");
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonProduct = objectMapper.writeValueAsString(product);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(PRODUCT_TOPIC, jsonProduct);
        producer.send(producerRecord).get();
        producer.close();
    }
}
