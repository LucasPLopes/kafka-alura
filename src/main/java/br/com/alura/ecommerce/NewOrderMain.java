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
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var producer = new KafkaProducer<String, String>(PropertiesConfig.producerProperties())) {
            var uuid = UUID.randomUUID().toString();
            var record = new ProducerRecord<>("LOJA_NOVO_PEDIDO", uuid, String.format("PEDIDO%S;%.2f", uuid, 100.0));
            producer.send(record).get();

            var record2 = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", uuid, String.format("PEDIDO%S;%.2f", uuid, 100.0));
            producer.send(record2, producerCallback()).get();
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
