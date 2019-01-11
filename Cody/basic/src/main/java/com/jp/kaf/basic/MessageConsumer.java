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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 *
 *
 *
 * @author dimit.chadha
 * @since 1.0
 * @version 1.0
 * @date Jan 11, 2019
 */
public class MessageConsumer
{

    private Scanner scanner;

    public static void main(String[] args) throws Exception
    {

        MessageConsumer messageConsumer = new MessageConsumer();
        messageConsumer.consumeMessage(args[0], args[1], Long.parseLong(args[2]));

//        if (argv.length != 2) {
//            System.err.printf("Usage: %s <topicName> <groupId>\n",
//                    Consumer.class.getSimpleName());
//            System.exit(-1);
//        }
    }

    private void consumeMessage(String topicName, String groupId, long startingOffSet) throws InterruptedException
    {
        scanner = new Scanner(System.in);

        ConsumerThread consumerRunnable = new ConsumerThread(topicName, groupId, startingOffSet);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit"))
        {
            line = scanner.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread
    {

        private String topicName;
        private String groupId;
        private long startingOffSet;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId, long startingOffSet)
        {
            this.topicName = topicName;
            this.groupId = groupId;
            this.startingOffSet = startingOffSet;
        }

        public void run()
        {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
            configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener()
            {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions)
                {
                    System.out.printf("%s topic-partitions are revoked from this consumer\n", Arrays.toString(partitions.toArray()));
                }

                public void onPartitionsAssigned(Collection<TopicPartition> partitions)
                {
                    System.out.printf("%s topic-partitions are assigned to this consumer\n", Arrays.toString(partitions.toArray()));
                    Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
                    while (topicPartitionIterator.hasNext())
                    {
                        TopicPartition topicPartition = topicPartitionIterator.next();
                        System.out.println("Current offset is " + kafkaConsumer.position(topicPartition) + " committed offset is ->" + kafkaConsumer.committed(topicPartition));
                        if (startingOffSet == -2)
                        {
                            System.out.println("Leaving it alone");
                        }
                        else if (startingOffSet == 0)
                        {
                            System.out.println("Setting offset to begining");

                            kafkaConsumer.seekToBeginning(partitions);
                        }
                        else if (startingOffSet == -1)
                        {
                            System.out.println("Setting it to the end ");

                            kafkaConsumer.seekToEnd(partitions);
                        }
                        else
                        {
                            System.out.println("Resetting offset to " + startingOffSet);
                            kafkaConsumer.seek(topicPartition, startingOffSet);
                        }
                    }
                }
            });
            //Start processing messages
            try
            {
                while (true)
                {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                    {
                        System.out.println(record.value());
                    }
                    if (startingOffSet == -2)
                    {
                        kafkaConsumer.commitSync();
                    }
                }
            }
            catch (WakeupException ex)
            {
                System.out.println("Exception caught " + ex.getMessage());
            }
            finally
            {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer()
        {
            return this.kafkaConsumer;
        }
    }
}
