package com.mph.core.producer;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.mph.core.commons.Payer;

public class ProducerApp {

	private static final String MSG_FORMAT = "[{0}] MPH-User-{1}";
	
	public static void main(String[] args) {
		Map<Payer, Integer> inputMap = parseInput(args);
		Producer<Long, String> producer = ProducerFactory.createProducer();
		inputMap.forEach((k,v) -> produce(producer, k, v));
	}

	private static Map<Payer, Integer> parseInput(String[] args) {
		Map<Payer, Integer> inputMap = null;
		if (args != null && args.length > 0) {
			inputMap = new HashMap<Payer, Integer>();
			for (int i = 0; i < args.length; i++) {
				String input = args[i];
				String payer = input.split("-")[0];
				int count = Integer.parseInt(input.split("-")[1]);
				inputMap.put(Payer.valueOf(payer.toUpperCase()), count);
			}
		}
		return inputMap;
	}

	private static void produce(Producer<Long, String> producer, Payer topic, int count) {
		for (int index = 0; index < count; index++) {
			String msg = MessageFormat.format(MSG_FORMAT, topic.name(), index);
			ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic.name(), msg);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("[Producer] Record sent with message " + msg + ", offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}

}
