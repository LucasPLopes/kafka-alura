package br.com.alura.ecommerce;

import br.com.alura.ecommerce.config.PropertiesConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.regex.Pattern;

public class LogService {

    private static final Logger log = LoggerFactory.getLogger(LogService.class);

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String,String>(PropertiesConfig.consumerProperties(LogService.class.getSimpleName()));

        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
        do {
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                log.info("Founded {} records", records.count());
                for (var record : records){
                    log.info("{}/ {}/ {}/ {}/ {}/", record.topic(),
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset());
                }
            }
        }while(true);
    }
}
