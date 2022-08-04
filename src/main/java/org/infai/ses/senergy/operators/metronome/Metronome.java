/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infai.ses.senergy.operators.metronome;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.models.AnalyticsMessageModel;
import org.infai.ses.senergy.models.MessageModel;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.FlexInput;
import org.infai.ses.senergy.operators.Helper;
import org.infai.ses.senergy.operators.Message;
import org.infai.ses.senergy.utils.StreamsConfigProvider;
import org.infai.ses.senergy.utils.TimeProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Metronome extends BaseOperator {

    private final boolean debug;
    private final KafkaProducer<String, String> producer;

    private final Map<String, Object> output = new HashMap<>();
    private final AnalyticsMessageModel messageModel = new MessageModel().getOutputMessage();

    private boolean initialMessageReceived = false;


    public Metronome(String outputTopic, long interval) {
        debug = Boolean.parseBoolean(Helper.getEnv("DEBUG", "false"));
        Properties props = StreamsConfigProvider.getStreamsConfiguration();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.get(StreamsConfig.APPLICATION_ID_CONFIG) + "-producer");
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

        ObjectMapper mapper = new ObjectMapper();
        ScheduledExecutorService ticker = Executors.newScheduledThreadPool(1);
        Runnable sendMessage = () -> {
            if (!initialMessageReceived) return;
            String t = TimeProvider.nowUTCToString();
            output.put("timestamp", t);
            messageModel.setAnalytics(output);
            messageModel.setTime(t);
            try {
                String val = mapper.writeValueAsString(messageModel);
                producer.send(new ProducerRecord<>(outputTopic, val));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        };
        ticker.scheduleAtFixedRate(sendMessage, 0, interval, TimeUnit.NANOSECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    @Override
    public void run(Message message) {
        initialMessageReceived = true;
        FlexInput valueInput = message.getFlexInput("value");
        try {
            final Object value = valueInput.getValue(Object.class);
            output.put("value", value);
            if (debug) {
                System.out.println("Value updated to " + value);
            }
        } catch (NoValueException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addFlexInput("value");
        return message;
    }
}
