package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.datatransfer.StringSelection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger log = LoggerFactory.getLogger(NewOrderMain.class);
        try (var producer = new KafkaProducer<String, String>(properties())) {
            var record = new ProducerRecord<String, String>("LOJA_NOVO_PEDIDO", String.format("PEDIDO%S;%.2f", UUID.randomUUID(), 100.0));
            producer.send(record).get();

            var record2 = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", String.format("PEDIDO%S;%.2f", UUID.randomUUID(), 100.0));
            producer.send(record2, producerCallback(log)).get();
        }

    }

    private static Callback producerCallback(Logger log) {
        return (data, ex) -> {
            if (ex != null) {
                log.error("Error: {}", ex.getMessage());
            }
            log.info("{} ::: partition: {}/ offset: {}/ timestamp {}", data.topic(), data.partition(), data.offset(), data.timestamp());
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
