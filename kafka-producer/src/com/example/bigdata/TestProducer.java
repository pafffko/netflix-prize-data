package com.example.bigdata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {
    public static void main(String[] args) {
//default settings
        String[] params = new String[]{
                "/tmp/netflix", //directory - 0
                "15", //sleepTime - 1
                "kafka-netflix", //topicName -2
                "1", //headerLength -3
                "0.0.0.0:6667"}; //bootstrapServers -4
        int i = 0;
        for (String arg : args) {
            params[i] = arg; i++;
        }
        Properties props = new Properties();

        props.put("bootstrap.servers", params[4]);

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linges.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final File folder = new File(params[0]);
        File[] listOfFiles = folder.listFiles();
        String listOfPaths[] = Arrays.stream(listOfFiles).
                map(file -> file.getAbsolutePath()).toArray(String[]::new);
        Arrays.sort(listOfPaths);
        for (final String fileName : listOfPaths)
        {
            try (Stream<String> stream = Files.lines(Paths.get(fileName)).skip(Integer.parseInt(params[3])))
            {
                stream.forEach(line -> producer.send(new ProducerRecord<String,
                        String>(params[2], String.valueOf(line.hashCode()), line)));

                TimeUnit.SECONDS.sleep(Integer.parseInt(params[1]));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}