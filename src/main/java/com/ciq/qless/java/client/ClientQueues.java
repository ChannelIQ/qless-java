package com.ciq.qless.java.client;

import java.util.Arrays;
import java.util.List;

import com.ciq.qless.java.Queue;

public class ClientQueues {
	public final JQlessClient _client;

	public ClientQueues(JQlessClient client) {
		this._client = client;
	}

	public int counts() {
		// parse JSON
		List<String> args = Arrays
				.asList(JQlessClient.getCurrentSeconds());

		return (Integer) this._client.call(JQlessClient.Command.QUEUES, args);
	}

	public Queue getNamedQueue(String queueName) {
		return new Queue(queueName, this._client, JQlessClient.getMachineName());
	}
}
