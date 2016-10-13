package org.wispersd.core.integration;

import org.apache.kafka.common.TopicPartition;

public interface ErrorHandlingStrategy {
	
	public ActionType handleError(TopicPartition currentPartition, long curPosition, Exception ex);

}
