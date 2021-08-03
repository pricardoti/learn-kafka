package br.com.pricardo.ecommerce;

public class EcommerceKafkaConstants {

    private EcommerceKafkaConstants() throws IllegalAccessException {
        throw new IllegalAccessException("Não é possivel instanciar esta classe");
    }

    public static final String KAFKA_HOST_CONNECTION = "localhost:9092";

    public static final String KAFKA_TOPIC_NEW_ORDER = "ECOMMERCE_TOPIC_NEW_ORDER";
    public static final String KAFKA_TOPIC_EMAIL = "ECOMMERCE_TOPIC_EMAIL";
}
