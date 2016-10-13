package org.wispersd.core.integration.kafka.consumer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerFactory {
	private List<String> topicNames;
	private Properties consumerProperties;
	private final ConcurrentMap<String, Consumer<?, ?>> topicConsumers = new ConcurrentHashMap<String, Consumer<?,?>>();
	
	
	public void setTopicNames(List<String> topicNames) {
		this.topicNames = topicNames;
	}
	
	
	
	
	public void setConsumerProperties(Properties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	public void init() {
		for(String nextTopic: topicNames) {
			getConsumerByTopicName(nextTopic);
		}
	}
	
	public void destroy() {
		for(Consumer<?,?> nextConsumer: topicConsumers.values()) {
			try {
				nextConsumer.close();
			} catch (Exception e) {
			}
		}
		topicConsumers.clear();
	}


	public Consumer<?, ?> getConsumerByTopicName(String topicName) {
		Consumer<?, ?> consumer = topicConsumers.get(topicName);
		if (consumer == null) {
			consumer = new KafkaConsumer(consumerProperties);
			Consumer<?,?> existing = topicConsumers.putIfAbsent(topicName, consumer);
			if (existing != null) {
				return existing;
			}
			else {
				return consumer;
			}
		}
		else {
			return consumer;
		}
	}
}
