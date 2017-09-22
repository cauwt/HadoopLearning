package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yachao on 17/9/10.
 */
public class KafkaConsumerEx {

    public static Properties getConfig() {
        Properties props = new Properties();
        props.setProperty("bootstrap.server", "localhost:9092");
        props.setProperty("group.id", "testGroup");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void consumeMessage() {
        int numOfConsumers = 3;
        final String topic = "test1";
        final ExecutorService executor = Executors.newFixedThreadPool(numOfConsumers);
        final List<KafkaConsumerRunner> consumers = new ArrayList<>();

        for (int i = 0; i < numOfConsumers; i++) {
            KafkaConsumerRunner consumer = new KafkaConsumerRunner(topic);
            consumers.add(consumer);
            executor.execute(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (KafkaConsumerRunner consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();

                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static class KafkaConsumerRunner implements Runnable {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private KafkaConsumer<String, String> consumer;
        private String topic;

        public KafkaConsumerRunner(String topic) {
            Properties props = getConfig();
            consumer = new KafkaConsumer<String, String>(props);
            this.topic = topic;
        }

        public void handleRecord(ConsumerRecord record) {
            System.out.println("name: " + Thread.currentThread().getName() + " ; topic: " + record.topic()
                    + " ; offset" + record.offset() + " ; key: " + record.key() + " ; value: " + record.value());
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Arrays.asList(topic));
                while (!closed.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(10000);

                    for (ConsumerRecord record : records) {
                        handleRecord(record);
                    }
                }
            } catch (WakeupException ex) {
                if (!closed.get()) {
                    throw ex;
                }
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            closed.set(true);
            consumer.close();
        }
    }

    public static void main(String[] args) {
        KafkaConsumerEx kafkaConsumerEx = new KafkaConsumerEx();
        kafkaConsumerEx.consumeMessage();
    }
}
