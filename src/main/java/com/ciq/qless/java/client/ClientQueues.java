package com.ciq.qless.java.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.utils.ResponseFactory;

public class ClientQueues {
	public final JQlessClient _client;

	public ClientQueues(JQlessClient client) {
		this._client = client;
	}

	public List<Map<String, Object>> counts() {
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds());

		return this._client.call(JQlessClient.Command.QUEUES, args).as(
				ResponseFactory.QUEUES);
	}

	public Queue getNamedQueue(String queueName) {
		return new Queue(queueName, this._client, JQlessClient.getMachineName());
	}
}
