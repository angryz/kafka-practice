package info.noconfuse.practice.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
 *
 * @author Zheng Zhipeng
 */
public class ConsumerGroupExample {

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public ConsumerGroupExample(String zookeeper, String groupId, String topic) {
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zookeeper, groupId)
        );
        this.topic = topic;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timeout waiting for consumer threads to shutdown, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted during shutdown, exiting uncleanly.");
        }
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        executor = Executors.newFixedThreadPool(numThreads);

        // now create an object to consume the message
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties prop = new Properties();
        prop.put("zookeeper.connect", zookeeper);
        prop.put("group.id", groupId);
        prop.put("zookeeper.session.timeout.ms", "500");
        prop.put("zookeeper.sync.time.ms", "200");
        prop.put("auto.offset.reset", "largest"); // if offset is out of range, then reset the
        // offset to the largest offset
        //prop.put("auto.commit.enable", "false"); // disable auto commit, zookeeper won't create
        // /consumers/group-name/offsets/topic-name/partition-num node
        prop.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(prop);
    }

    private static class ConsumerTest implements Runnable {

        private KafkaStream stream;
        private int threadNumber;

        public ConsumerTest(KafkaStream stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                System.out.println("Thread " + threadNumber + ": " + new String(it.next().message()));
            }
            System.out.println("Shutting down Thread: " + threadNumber);
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Arguments required: Zookeeper GroupID Topic ThreadNumber");
            System.exit(0);
        }
        String zk = args[0];
        String groupId = args[1];
        String topic = args[2];
        int threads = Integer.parseInt(args[3]);

        ConsumerGroupExample example = new ConsumerGroupExample(zk, groupId, topic);
        example.run(threads);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {

        }
        example.shutdown();
    }
}
