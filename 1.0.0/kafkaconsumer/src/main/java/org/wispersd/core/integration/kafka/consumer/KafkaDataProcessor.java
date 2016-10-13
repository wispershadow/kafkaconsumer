package org.wispersd.core.integration.kafka.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wispersd.core.integration.ActionType;
import org.wispersd.core.integration.DataProcessDelegate;
import org.wispersd.core.integration.ErrorHandlingStrategy;
import org.wispersd.core.integration.Tuple;

public class KafkaDataProcessor<K, T, R> implements Runnable{
	private static final Logger logger = LoggerFactory.getLogger(KafkaDataProcessor.class);
	private final CountDownLatch countDownLatch;

	private final List<Tuple<K, T>> dataList;

	private final TopicPartition currentPartition;

	private final Map<TopicPartition, OffsetAndMetadata> offsetToCommit;

	private long currentPosition;
	
	private volatile DataProcessDelegate<K, T, R> processDelegate;
	
	private volatile ErrorHandlingStrategy errorHandlingStrategy;
	

	public KafkaDataProcessor(CountDownLatch countDownLatch, List<Tuple<K, T>> dataList,
			TopicPartition currentPartition,
			Map<TopicPartition, OffsetAndMetadata> offsetToCommit, long initPosition) {
		this.countDownLatch = countDownLatch;
		this.dataList = dataList;
		this.currentPartition = currentPartition;
		this.offsetToCommit = offsetToCommit;
		this.currentPosition = initPosition;
	}


	public void initProcessDelegate(DataProcessDelegate<K, T, R> processDelegate) {
		this.processDelegate = processDelegate;
	}
	
	public void initErrorHandlingStrategy(ErrorHandlingStrategy errorHandlingStrategy) {
		this.errorHandlingStrategy = errorHandlingStrategy;
	}


	public void run() {
		for(Tuple<K, T> nextData: dataList) {
			try {
				processDelegate.execute(currentPartition.topic(), nextData.getKey(), nextData.getValue());
				currentPosition++;
			}
			catch(Exception ex) {
				ex.printStackTrace();
				logger.error("Error process " + currentPartition + "  position: " + currentPosition, ex);
				ActionType actionType = errorHandlingStrategy.handleError(currentPartition, currentPosition, ex);
				if (actionType == ActionType.PROCEED) {
					currentPosition++;
				}
				else {
					break;
				}
			}
		}
		offsetToCommit.put(currentPartition, new OffsetAndMetadata(currentPosition));
		countDownLatch.countDown();
	}

}
