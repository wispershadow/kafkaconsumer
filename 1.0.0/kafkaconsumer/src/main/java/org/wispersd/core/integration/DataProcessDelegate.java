package org.wispersd.core.integration;

public interface DataProcessDelegate<K, T, R> {
	
	public R execute(String topic, K key, T param) throws Exception;

}
