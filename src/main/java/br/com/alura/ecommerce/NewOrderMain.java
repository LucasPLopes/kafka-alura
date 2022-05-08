package br.com.alura.ecommerce;

import br.com.alura.ecommerce.config.PropertiesConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    private static final Logger log = LoggerFactory.getLogger(NewOrderMain.class);

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++)
            try (var producer = new KafkaProducer<String, String>(PropertiesConfig.producerProperties())) {
                var key = UUID.randomUUID().toString();
                var value = String.format("PEDIDO%S;%.2f", key, Math.random() % 150);
                var record = new ProducerRecord<>("LOJA_NOVO_PEDIDO", key, value);
                var record2 = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
                var email = "Thank you for your order! We are processing your order!";
                var record3 = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

                producer.send(record, producerCallback()).get();
                producer.send(record2, producerCallback()).get();
                producer.send(record3, producerCallback()).get();
            } catch (Exception e) {
                log.error("NewOrderMain error: {}", e.getMessage());
            }

    }

    private static Callback producerCallback() {
        return (data, ex) -> {
            if (ex != null) {
                NewOrderMain.log.error("Error: {}", ex.getMessage());
            }
            NewOrderMain.log.info("{} ::: partition: {}/ offset: {}/ timestamp {}", data.topic(), data.partition(), data.offset(), data.timestamp());
        };
    }

}
