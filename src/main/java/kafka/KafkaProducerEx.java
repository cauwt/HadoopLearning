package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by yachao on 17/9/10.
 */
public class KafkaProducerEx {

    public Properties getConfig() {
        Properties props = new Properties();
        props.setProperty("bootstrap.server", "localhost:9092");
        props.setProperty("acks", "all");
        props.setProperty("retries", "0");
        props.setProperty("batch.size", "16384");
        props.setProperty("linger.ms", "1");
        props.setProperty("buffer.memory", "33554432");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void produceMessage() {
        Properties props = getConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 1; i < 1000; i++) {
            producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), Integer.toString(i)));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        producer.close();
    }

    public static void main(String[] args) {
        KafkaProducerEx kafkaProducerEx = new KafkaProducerEx();
        kafkaProducerEx.produceMessage();
    }
}
