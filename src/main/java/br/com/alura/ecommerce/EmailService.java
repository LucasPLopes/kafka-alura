package br.com.alura.ecommerce;

import br.com.alura.ecommerce.config.PropertiesConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    private static final Logger log = LoggerFactory.getLogger(EmailService.class);

    public static void main(String[] args) {
        Properties properties = PropertiesConfig.consumerProperties(EmailService.class.getSimpleName());

        properties.remove(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        properties.remove(ConsumerConfig.CLIENT_ID_CONFIG);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));

            do {
                var records = consumer.poll(Duration.ofMillis(200));
                if (!records.isEmpty()) {
                    log.info("Founded {} records", records.count());
                    for (var record : records) {
                        log.info("SEND {}/ {}/ {}/ {}/", record.key(),
                                record.value(),
                                record.partition(),
                                record.offset());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            log.error(e.getMessage());
                        }
                        log.info("Email sent");
                    }
                }

            } while (true);
        }


    }
}
