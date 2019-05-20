/*
 * Copyright (c) Java Pathshala
 * Wisdom Being Shared
 * All rights reserved.
 *
 * No parts of this source code can be reproduced without written consent from
 * Java Pathshala
 * JavaPathshala.com
 *
 */
package com.jp.kaf.basic;

import java.io.IOException;
import static java.lang.System.in;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 *
 *
 * @author dimit.chadha
 * @since 1.0
 * @version 1.0
 * @date Jan 11, 2019
 */
public class MessageProducer
{

    private Scanner scanner;

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        MessageProducer messageProducer = new MessageProducer();
        messageProducer.produceMessage("JP");
    }

    /**
     *
     * @param topicName
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private void produceMessage(String topicName) throws IOException
    {
        scanner = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer(configProperties);

        String line = scanner.nextLine();
        while (!line.equals("exit"))
        {
            //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, null, line);
            producer.send(rec, new Callback()
            {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception excptn)
                {
                    System.out.println("Message sent to topic ->" + metadata.topic() + " stored at offset->" + metadata.offset());
                }
            });
            line = scanner.nextLine();
        }
        in.close();
        producer.close();

    }
}
