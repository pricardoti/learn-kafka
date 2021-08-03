package br.com.pricardo.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static br.com.pricardo.ecommerce.EcommerceKafkaConstants.KAFKA_HOST_CONNECTION;
import static br.com.pricardo.ecommerce.EcommerceKafkaConstants.KAFKA_TOPIC_EMAIL;
import static br.com.pricardo.ecommerce.EcommerceKafkaConstants.KAFKA_TOPIC_NEW_ORDER;
import static java.util.Objects.nonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class NewOrder {

    private static final Logger logger = LoggerFactory.getLogger(NewOrder.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var idMessage = UUID.randomUUID().toString();

        sendMessage(producer, new ProducerRecord<>(KAFKA_TOPIC_NEW_ORDER, idMessage, "Message test new order send: " + LocalDateTime.now()));
        sendMessage(producer, new ProducerRecord<>(KAFKA_TOPIC_EMAIL, idMessage, "client@email.com"));
    }

    private static void sendMessage(
            KafkaProducer<String, String> producer,
            ProducerRecord<String, String> record
    ) throws InterruptedException, ExecutionException {
        producer.send(record, callback()).get();
    }

    private static Callback callback() {
        return (data, ex) -> {
            if (nonNull(ex)) {
                ex.printStackTrace();
                return;
            }
            logger.info("..:: SEND MESSAGE ::: Topic: " + data.topic() + " | Offset: " + data.offset() + " | Partition: " + data.partition() + " | Timestamp: " + data.timestamp() + " ::..");
        };
    }

    /**
     * Informacoes para configuracao e execucao do Kafka/Topico, em aplicacoes reais essas informacoes normalmente sao
     * lidas a partir de um arquivo de propriedades (.properties ou .yaml) - este apenas uma exemplo para fins didatico.
     */
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST_CONNECTION);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
