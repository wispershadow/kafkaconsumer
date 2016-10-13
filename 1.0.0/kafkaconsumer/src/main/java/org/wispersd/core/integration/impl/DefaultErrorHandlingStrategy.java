package org.wispersd.core.integration.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.TopicPartition;
import org.wispersd.core.integration.ActionType;
import org.wispersd.core.integration.ErrorHandlingStrategy;


public class DefaultErrorHandlingStrategy implements ErrorHandlingStrategy {
	private int maxRetryTimes = 3;
	private List<Class<? extends Exception>> skipExceptionList;
	private final ConcurrentMap<String, AtomicInteger> retryCountMap = new ConcurrentHashMap<String, AtomicInteger>();

	public int getMaxRetryTimes() {
		return maxRetryTimes;
	}


	public void setMaxRetryTimes(int maxRetryTimes) {
		this.maxRetryTimes = maxRetryTimes;
	}


	public List<Class<? extends Exception>> getSkipExceptionList() {
		return skipExceptionList;
	}


	public void setSkipExceptionList(
			List<Class<? extends Exception>> skipExceptionList) {
		this.skipExceptionList = skipExceptionList;
	}


	public ActionType handleError(TopicPartition currentPartition,
			long curPosition, Exception ex) {
		if (toSkip(ex)) {
			return ActionType.PROCEED;
		}
		else {
			String key = formKey(currentPartition, curPosition);
			AtomicInteger curCount = retryCountMap.get(key);
			if (curCount == null) {
				curCount = new AtomicInteger(0);
				AtomicInteger existing = retryCountMap.putIfAbsent(key, curCount);
				if (existing != null) {
					curCount = existing;
				}
			}
			
			if (curCount.get() > maxRetryTimes) {
				retryCountMap.remove(key);
				System.out.println("Discard " + ex.getMessage() + " after max retry");
				return ActionType.PROCEED;
			}
			else {
				curCount.incrementAndGet();
				return ActionType.RETRY;
			}
		}
	}

	protected String formKey(TopicPartition partition, long position) {
		StringBuilder sb = new StringBuilder();
		sb.append(partition.topic());
		sb.append(".");
		sb.append(partition.partition());
		sb.append(".");
		sb.append(position);
		return sb.toString();
	}
	
	
	protected boolean toSkip(Exception ex) {
		if (skipExceptionList != null && (!skipExceptionList.isEmpty())) {
			for(Class<? extends Exception> nextExceptionClass: skipExceptionList) {
				if (ex.getClass().isAssignableFrom(nextExceptionClass)) {
					return true;
				}
			}
		}
		return false;
	}
}
