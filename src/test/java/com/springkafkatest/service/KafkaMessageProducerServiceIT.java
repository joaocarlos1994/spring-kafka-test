package com.springkafkatest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafkatest.config.KafkaTestConfiguration;
import com.springkafkatest.model.event.UpdatedBrandEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;


@DirtiesContext
@RunWith(SpringRunner.class)
@EmbeddedKafka(partitions = 1)
@SpringBootTest(classes = KafkaTestConfiguration.class)
public class  KafkaMessageProducerServiceIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducerServiceIT.class);

    private static String TOPIC_NAME = "UpdatedBrandEvent";

    @Autowired
    private KafkaMessageProducerService kafkaMessageProducerService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaMessageListenerContainer<String, UpdatedBrandEvent> container;

    private BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

    @Before
    public void setUp() {
        consumerRecords = new LinkedBlockingQueue<>();

        final var containerProperties = new ContainerProperties(TOPIC_NAME);
        final var consumerProperties = KafkaTestUtils.consumerProps("sender", "false", embeddedKafkaBroker);
        final var consumer = new DefaultKafkaConsumerFactory<>(consumerProperties);

        container = new KafkaMessageListenerContainer<>(consumer, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> {
            LOGGER.debug("Listened message='{}'", record.toString());
            consumerRecords.add(record);
        });
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        container.stop();
    }

    @Test
    public void it_should_send_updated_brand_event() throws InterruptedException, IOException {
        UpdatedBrandEvent updatedBrandEvent = new UpdatedBrandEvent(
                "BrandMapCreatedEvent", "Brand1", "FIRM_A");
        kafkaMessageProducerService.send(updatedBrandEvent);

        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString( updatedBrandEvent );

        assertThat(received, hasValue(json));

        assertThat(received).has(key(null));
    }

}