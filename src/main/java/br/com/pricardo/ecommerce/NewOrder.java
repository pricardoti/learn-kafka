package br.com.pricardo.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.nonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class NewOrder {

    private static final Logger logger = LoggerFactory.getLogger(NewOrder.class);

    private static final String HOST_CONNECTION = "localhost:9092";
    private static final String KAFKA_TOPIC_DEFAULT = "ECOMMERCE_NEW_ORDER";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());

        var idMessage = UUID.randomUUID().toString();
        var record = new ProducerRecord<>(KAFKA_TOPIC_DEFAULT, idMessage, "Message Send: " + LocalDateTime.now());

        sendMessage(producer, record);
    }

    private static void sendMessage(
            KafkaProducer<String, String> producer,
            ProducerRecord<String, String> record
    ) throws InterruptedException, ExecutionException {
        producer.send(record, (data, ex) -> {
            if (nonNull(ex)) {
                ex.printStackTrace();
                return;
            }
            logger.info("..:: DATA MESSAGE ::: Topic: " + data.topic() + " | Offset: " + data.offset() + " | Partition: " + data.partition() + " | Timestamp: " + data.timestamp() + " ::..");
        }).get();
    }

    /**
     * Informacoes para configuracao e execucao do Kafka/Topico, em aplicacoes reais essas informacoes normalmente sao
     * lidas a partir de um arquivo de propriedades (.properties ou .yaml) - este apenas uma exemplo para fins didatico.
     */
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, HOST_CONNECTION);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
