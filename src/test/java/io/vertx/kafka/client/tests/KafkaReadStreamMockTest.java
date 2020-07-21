package io.vertx.kafka.client.tests;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;

@RunWith(VertxUnitRunner.class)
public class KafkaReadStreamMockTest extends KafkaTestBase {

    private LinkedList<ConsumerRecord<String,String>> recordsMock = new LinkedList<>();

    private int SEND_BATCH = 5;
    private int TOTAL_MESSAGES = 400;
    private final String TOPIC = "topic";
    private Long timer = null;

    private void initRecords(){
        int numMessages = TOTAL_MESSAGES;
        for (int i = 0;i < numMessages;i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            recordsMock.add(new ConsumerRecord<String,String>(TOPIC, 0, i, key, value));
        }
    }

    private MockConsumer<String, String> createMockConsumer(){
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        Map<org.apache.kafka.common.TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new org.apache.kafka.common.TopicPartition(TOPIC, 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
        return consumer;
    }

    private void sendNextBatch(MockConsumer<String, String> consumer){
        for(int i=0;i<SEND_BATCH && recordsMock.size()>0;i++)
        consumer.addRecord(recordsMock.pop());

    }

    @Test
    public void shouldNotLoseMessages(TestContext ctx){
        Vertx vertx = Vertx.vertx();

        Async done = ctx.async();

        initRecords();

        MockConsumer<String, String> consumer = createMockConsumer();
        KafkaReadStream<String, String> readStream =  KafkaReadStream.create(vertx, consumer);
        KafkaConsumer<String, String> consumerVertx = new KafkaConsumerImpl<>(readStream);


        AtomicLong partitionOffset = new AtomicLong(-1);


        consumerVertx.handler((r)->{
            long offset = r.offset();

            partitionOffset.addAndGet(1);
            ctx.assertEquals(partitionOffset.get(), offset);

            if(offset == TOTAL_MESSAGES-1){
                consumerVertx.close();
                done.complete();
            } else {

                if(timer!=null) vertx.cancelTimer(timer);
                timer = vertx.setTimer(5, (t)->{
                    consumerVertx.pause();
                    ctx.assertEquals(0L, consumerVertx.demand());
                    vertx.getOrCreateContext().runOnContext((t1)->{
                        consumerVertx.commit();
                        consumerVertx.resume();
                        ctx.assertEquals(Long.MAX_VALUE, consumerVertx.demand());
                        sendNextBatch(consumer);
                        // sends two batches of messages
                        vertx.getOrCreateContext().runOnContext((t2)->{
                            sendNextBatch(consumer);
                        });
                    });
                });

            }

        });

        consumerVertx.exceptionHandler(t->ctx.fail(t));


        Set<TopicPartition> partitions = new LinkedHashSet<>();
        partitions.add(new TopicPartition(TOPIC, 0));

        consumerVertx.assign(partitions, (h)->{
            sendNextBatch(consumer);
        });

    }

}
