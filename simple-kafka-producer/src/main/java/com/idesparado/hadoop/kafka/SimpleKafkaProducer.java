package com.idesparado.hadoop.kafka;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleKafkaProducer {
	private static Random rnd;

	static {
		rnd = new Random(System.nanoTime());
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			printUsage();
			System.exit(1);
		}

		String broker = args[0];
		String topic = args[1];
		int count = Integer.valueOf(args[2]);
		int delay = Integer.valueOf(args[3]);

		Properties props = new Properties();

		props.put("metadata.broker.list", broker);
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ProducerConfig pConfig = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(pConfig);

		for (int i = 0; i < count; ++i) {
			String json = createRandomJson();
			System.out.println(String.format("%d - %s", i, json));
			producer.send(new KeyedMessage<String, String>(topic, json));
			Thread.sleep(delay);
		}
	}

	private static String createRandomJson() {
		return String.format("{\"num\":%d, \"name\":\"%s\"}", rnd.nextInt(1000), UUID.randomUUID().toString());
	}

	private static void printUsage() {
		System.err.println("Usage : " + SimpleKafkaProducer.class.getName() + " <broker> <topic> <count> <delay>");
	}
}
