package com.airing;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class KafkaTest {

    private String topic = "kafka_items";

    @Test
    public void print() {
        System.out.println("kafka");
    }

    /**
     * kafka-topics.sh --zookeeper localhost:2181/kafka --create --topic kafka_items --partitions 2
     * --replication-factor 1
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void producer() throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.25.133:9093,172.16.25.133:9094");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        while (true) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key" + j, "value" + i);
                    Future<RecordMetadata> send = producer.send(record);
                    RecordMetadata recordMetadata = send.get();
                    int partition = recordMetadata.partition();
                    long offset = recordMetadata.offset();
                    System.out.println("key: " + record.key() + ", value: " + record.value() + ", partition: "
                            + partition + ", offset: " + offset);
                }
            }
        }

    }

    @Test
    public void consumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.25.133:9093,172.16.25.133:9094");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka1");
        /**
         * What to do when there is no initial offset in Kafka
         * or if the current offset does not exist any more on the server (e.g. because that data has been deleted):
         * <ul>
         *     <li>earliest: automatically reset the offset to the earliest offset
         *     <li>latest: automatically reset the offset to the latest offset</li>
         *     <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>
         *     <li>anything else: throw exception to the consumer.</li>
         * </ul>
         */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 异步提交，可能会重复消费或者丢失消息
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "15000");
//        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, );

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // kafka的consumer会动态负载均衡
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        });

        /**
         * 如果手动提交offset
         * 1. 按消息进度同步提交
         * 2. 按分区粒度同步提交
         * 3. 按当前poll的批次同步提交
         *
         * 思考：如果在多个线程下
         * 1，3的方式不用多线程
         * 2的方式最容易想到多线程方式处理，有没有问题？每个分区一个线程没有问题
         */

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (records.isEmpty()) {
//                System.out.println(0);
                continue;
            }
            System.out.println("----------" + records.count() + "------------");

            // 每次poll取多个分区时
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> recordList = records.records(partition);
                Iterator<ConsumerRecord<String, String>> iterator = recordList.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    int par = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();

                    TopicPartition tp = new TopicPartition(topic, par);
                    OffsetAndMetadata om = new OffsetAndMetadata(offset);
                    Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    map.put(tp, om);
                    // 这个是最安全的，每条记录级的更新
                    consumer.commitSync(map);
                }

                // 获取分区内最后一条消息的offset
                long poff = recordList.get(recordList.size() - 1).offset();
                OffsetAndMetadata pom = new OffsetAndMetadata(poff);
                HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(partition, pom);
                // 分区粒度提交offset
                consumer.commitSync(map);
            }

            consumer.commitSync();




            /*Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                int partition = record.partition();
                long offset = record.offset();
                System.out.println("key: " + record.key() + ", value: " + record.value() + ", partition: "
                        + partition + ", offset: " + offset);
            }*/
        }
    }

}
