package br.com.alura.ecommerce;

import br.com.alura.ecommerce.config.PropertiesConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

import static java.lang.Thread.sleep;

public class FraudDetectorService {
    private static final Logger log = LoggerFactory.getLogger(FraudDetectorService.class);

    public static void main(String[] args) {
        try (var consumer = new KafkaConsumer<String, String>(PropertiesConfig.consumerProperties(FraudDetectorService.class.getSimpleName()))) {
            consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                if (!records.isEmpty()) {
                    log.info("Find {} records", records.count());

                    for (var record : records) {
                        log.info("key: {}/ value: {}/ partition: {}/ offset: {}", record.key(), record.value(), record.partition(), record.offset());
                        try {
                            sleep(5000);
                        } catch (InterruptedException e) {
                            log.error(e.getMessage());
                        }
                        log.info("Order processed");
                    }
                }
            } while (true);
        }
    }
}
