package com.shopping.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shopping.kafka.model.Product;
import okhttp3.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 *  The ProductConsumer class implements a Kafka consumer that consumes product messages from a Kafka topic.
 *  It retrieves product data from an API and performs actions based on the received products.
 */
public class ProductConsumer {
    private static final Logger log = LoggerFactory.getLogger(ProductConsumer.class);
    private static final String PRODUCT_TOPIC = "products";
    private static final String PRODUCTS_API_URL = "http://localhost:8082/api/products";
    private static final OkHttpClient client = new OkHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String USERNAME = "cart-user";
    private static final String PASSWORD = "123456";

    /**
     * Entry point of the application. Consumes products from a Kafka topic, processes them, and updates the corresponding products in an API.
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "product-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(PRODUCT_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String jsonProduct = record.value();
                try {
                    Product product = objectMapper.readValue(jsonProduct, Product.class);
                    log.info("Received product: " + product);
                    log.info("Received product Name: " + product.getName());

                    handleProduct(product);
                } catch (Exception e) {
                    log.error("Failed to process record", e);
                }
            }
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                log.error("Commit failed", e);
            }
        }
    }

    private static void handleProduct(Product product) throws IOException {
        Request request = new Request.Builder()
                .url(PRODUCTS_API_URL)
                .addHeader("Authorization", basicAuth(USERNAME, PASSWORD))
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }

        String responseData = response.body().string();
        List<Product> products = Arrays.asList(objectMapper.readValue(responseData, Product[].class));

        boolean productExists = products.stream().anyMatch(p -> p.getId() == product.getId());

        if (productExists) {
            Product existingProduct = products.stream().filter(p -> p.getId() == product.getId()).findFirst().get();
            if (!existingProduct.equals(product)) {
                log.info("Updating product: " + product.getId());
                updateProduct(product);
            }
        } else {
            log.info("Adding new product: " + product.getId());
            addProduct(product);
        }
    }

    private static void updateProduct(Product product) throws IOException {
        String productJson = objectMapper.writeValueAsString(product);
        RequestBody body = RequestBody.create(productJson, MediaType.get("application/json"));
        Request request = new Request.Builder()
                .url(PRODUCTS_API_URL + "/" + product.getId())
                .put(body)
                .addHeader("Authorization", basicAuth(USERNAME, PASSWORD))
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }
    }

    private static void addProduct(Product product) throws IOException {
        String productJson = objectMapper.writeValueAsString(product);
        RequestBody body = RequestBody.create(productJson, MediaType.get("application/json"));
        Request request = new Request.Builder()
                .url(PRODUCTS_API_URL)
                .post(body)
                .addHeader("Authorization", basicAuth(USERNAME, PASSWORD))
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }
    }

    private static String basicAuth(String username, String password) {
        String credentials = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }
}