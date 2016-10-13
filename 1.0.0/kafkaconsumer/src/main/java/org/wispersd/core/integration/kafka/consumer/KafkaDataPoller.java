package org.wispersd.core.integration.kafka.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.wispersd.core.integration.DataProcessDelegate;
import org.wispersd.core.integration.ErrorHandlingStrategy;
import org.wispersd.core.integration.Tuple;


public class KafkaDataPoller<K, V, T, R> implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaDataPoller.class);
	private Consumer<K, V> kafkaConsumer;
	private String topicName;
	private Converter<ConsumerRecord<K, V>, T> consumerDataReverseConverter;
	private ExecutorService executorService;
	private DataProcessDelegate<K, T, R> processDelegate;
	private ErrorHandlingStrategy errorHandlingStrategy;
	
	private volatile boolean running = true;
	
	

	public Consumer<K, V> getKafkaConsumer() {
		return kafkaConsumer;
	}

	public void setKafkaConsumer(Consumer<K, V> kafkaConsumer) {
		this.kafkaConsumer = kafkaConsumer;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public Converter<ConsumerRecord<K, V>, T> getConsumerDataReverseConverter() {
		return consumerDataReverseConverter;
	}

	public void setConsumerDataReverseConverter(
			Converter<ConsumerRecord<K, V>, T> consumerDataReverseConverter) {
		this.consumerDataReverseConverter = consumerDataReverseConverter;
	}

	public DataProcessDelegate<K, T, R> getProcessDelegate() {
		return processDelegate;
	}

	public void setProcessDelegate(DataProcessDelegate<K, T, R> processDelegate) {
		this.processDelegate = processDelegate;
	}

	public ErrorHandlingStrategy getErrorHandlingStrategy() {
		return errorHandlingStrategy;
	}

	public void setErrorHandlingStrategy(ErrorHandlingStrategy errorHandlingStrategy) {
		this.errorHandlingStrategy = errorHandlingStrategy;
	}

	public ExecutorService getExecutorService() {
		return executorService;
	}

	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}	
	
	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
		if (!running) {
			notifyConsumerClose();
		}
	}
	
	public void destroy() {
		executorService.shutdown();
		try {
			executorService.awaitTermination(3000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
	}

	public void run() {
		kafkaConsumer.subscribe(Collections.singletonList(topicName));
		final Map<TopicPartition, OffsetAndMetadata> offsetToCommit = new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>();
		while (running && (!Thread.currentThread().isInterrupted()))
		{
			try
			{
				offsetToCommit.clear();
				final ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(1000);
				final int partitionCount = consumerRecords.partitions().size();
				final CountDownLatch countDownLatch = new CountDownLatch(partitionCount);
				for (final TopicPartition partition : consumerRecords.partitions())
				{
					final List<ConsumerRecord<K, V>> partitionRecords = consumerRecords.records(partition);
					if (logger.isDebugEnabled())
					{
						logger.debug("For partition, record size={}, partition={}", partitionRecords.size(), partition.partition());
					}
					final List<Tuple<K,T>> dataList = new ArrayList<Tuple<K, T>>();
					long initPos = -1;
					for (final ConsumerRecord<K, V> nextPartitionRec : partitionRecords)
					{
						if (initPos == -1)
						{
							initPos = nextPartitionRec.offset();
						}
						dataList.add(new Tuple<K, T>(nextPartitionRec.key(), consumerDataReverseConverter.convert(nextPartitionRec)));
					}
					final KafkaDataProcessor<K, T, R> processor = new KafkaDataProcessor<K, T, R>(countDownLatch, dataList, partition, offsetToCommit, initPos);
					processor.initProcessDelegate(processDelegate);
					processor.initErrorHandlingStrategy(errorHandlingStrategy);
					executorService.submit(processor);
				}
				countDownLatch.await();
				kafkaConsumer.commitSync(offsetToCommit);
				for (final TopicPartition partition : consumerRecords.partitions())
				{
					kafkaConsumer.seek(partition, offsetToCommit.get(partition).offset());
					//kafkaConsumer.seekToBeginning(partition);
				}
			}
			catch (final WakeupException we)
			{
				Thread.currentThread().interrupt();
			}
			catch (final InterruptedException ie)
			{
				Thread.currentThread().interrupt();
			}
			catch (final Exception ex) {
				logger.error("Error polling kafka data, terminating",  ex);
				running = false;
			}
		}
		kafkaConsumer.close();
	}
	
	
	protected void notifyConsumerClose()
	{
		kafkaConsumer.wakeup();
	}

}
