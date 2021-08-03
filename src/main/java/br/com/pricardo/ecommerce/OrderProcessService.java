package br.com.pricardo.ecommerce;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

import static br.com.pricardo.ecommerce.EcommerceKafkaConstants.KAFKA_HOST_CONNECTION;
import static br.com.pricardo.ecommerce.EcommerceKafkaConstants.KAFKA_TOPIC_NEW_ORDER;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class OrderProcessService {

    private static final Logger logger = LoggerFactory.getLogger(OrderProcessService.class);

    private static final String ORDER_TOPIC_GROUP = "ECOMMERCE_ORDER_PROCESS";
    private static final Duration CONSUMER_TIME_POLL = Duration.ofMillis(3000);

    public static void main(String[] args) throws InterruptedException {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(List.of(KAFKA_TOPIC_NEW_ORDER));

        while (true) {
            var records = consumer.poll(CONSUMER_TIME_POLL);
            logger.info("::: [" + LocalDateTime.now() + "] Listening message pool :::");
            records.forEach(record -> {
                logger.info("..:: --- Begin process new order --- ::..");
                logger.info(":: Order Key: " + record.key());
                logger.info(":: Order value: " + record.value());
                logger.info(":: Order topic: " + record.topic());
                logger.info(":: Order partition: " + record.partition());
                logger.info(":: Order offset: " + record.offset());
                logger.info(":: Order timestamp: " + record.timestamp());
                logger.info("..:: --- End process new order --- ::..");
            });
            Thread.sleep(10000);
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST_CONNECTION);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, ORDER_TOPIC_GROUP);
        return properties;
    }
}
