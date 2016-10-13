package org.wispersd.core.integration.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.convert.converter.Converter;
import org.wispersd.core.integration.DataProcessDelegate;
import org.wispersd.core.integration.ErrorHandlingStrategy;

public class KafkaDataPollerFactory {
	private ExecutorService executorService;
	private List<String> topicNames;
	private KafkaConsumerFactory kafkaConsumerFactory;
	private Map<String, Converter<ConsumerRecord<?, ?>,?>> topicDataConverters;
	private Map<String, DataProcessDelegate<?, ? , ?>> topicProcessDelegates;
	private Map<String, ErrorHandlingStrategy> topicErrorHandlingStrategies;
	
	private final Map<String, KafkaDataPoller<?,?,?,?>> kafkaDataPollers = new HashMap<String, KafkaDataPoller<?,?,?,?>>();
	
	
	
	
	public ExecutorService getExecutorService() {
		return executorService;
	}


	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}


	public List<String> getTopicNames() {
		return topicNames;
	}


	public void setTopicNames(List<String> topicNames) {
		this.topicNames = topicNames;
	}


	public KafkaConsumerFactory getKafkaConsumerFactory() {
		return kafkaConsumerFactory;
	}


	public void setKafkaConsumerFactory(KafkaConsumerFactory kafkaConsumerFactory) {
		this.kafkaConsumerFactory = kafkaConsumerFactory;
	}


	public Map<String, Converter<ConsumerRecord<?, ?>, ?>> getTopicDataConverters() {
		return topicDataConverters;
	}


	public void setTopicDataConverters(
			Map<String, Converter<ConsumerRecord<?, ?>, ?>> topicDataConverters) {
		this.topicDataConverters = topicDataConverters;
	}


	public Map<String, DataProcessDelegate<?, ?, ?>> getTopicProcessDelegates() {
		return topicProcessDelegates;
	}


	public void setTopicProcessDelegates(
			Map<String, DataProcessDelegate<?, ?, ?>> topicProcessDelegates) {
		this.topicProcessDelegates = topicProcessDelegates;
	}


	public Map<String, ErrorHandlingStrategy> getTopicErrorHandlingStrategies() {
		return topicErrorHandlingStrategies;
	}


	public void setTopicErrorHandlingStrategies(
			Map<String, ErrorHandlingStrategy> topicErrorHandlingStrategies) {
		this.topicErrorHandlingStrategies = topicErrorHandlingStrategies;
	}


	public void init() {
		for(String nextTopic: topicNames) {
			Consumer nextKafkaConsumer = kafkaConsumerFactory.getConsumerByTopicName(nextTopic);
			KafkaDataPoller nextPoller = new KafkaDataPoller();
			nextPoller.setKafkaConsumer(nextKafkaConsumer);
			nextPoller.setExecutorService(executorService);
			nextPoller.setTopicName(nextTopic);
			nextPoller.setConsumerDataReverseConverter(topicDataConverters.get(nextTopic));
			nextPoller.setProcessDelegate(topicProcessDelegates.get(nextTopic));
			nextPoller.setErrorHandlingStrategy(topicErrorHandlingStrategies.get(nextTopic));
			kafkaDataPollers.put(nextTopic, nextPoller);
			Thread t = new Thread(nextPoller);
			t.start();
		}
	}
	

	public void destroy() {
		for(KafkaDataPoller<?,?,?,?> nextPoller:kafkaDataPollers.values()) {
			nextPoller.setRunning(false);
			nextPoller.destroy();
		}
		kafkaDataPollers.clear();
	}
}
