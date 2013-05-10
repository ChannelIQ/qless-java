package com.ciq.qless.java.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ciq.qless.java.utils.ResponseFactory;

public class ClientWorkers {
	public final JQlessClient _client;

	public ClientWorkers(JQlessClient client) {
		this._client = client;
	}

	public List<Map<String, Object>> counts() {
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds());

		return this._client.call(JQlessClient.Command.WORKERS, args).as(
				ResponseFactory.WORKERS);
	}

	public Map<String, Object> getWorkersByName(String name) {
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds(),
				name);

		return this._client.call(JQlessClient.Command.WORKERS, args).as(
				ResponseFactory.WORKER);
	}
}
