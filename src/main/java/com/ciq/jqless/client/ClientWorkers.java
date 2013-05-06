package com.ciq.jqless.client;

import java.util.Arrays;
import java.util.List;

public class ClientWorkers {
	public final JQlessClient _client;

	public ClientWorkers(JQlessClient client) {
		this._client = client;
	}

	public int count() {
		// parse JSON
		List<String> args = Arrays
				.asList(JQlessClient.getCurrentSeconds());

		return (Integer) this._client.call(JQlessClient.Command.WORKERS, args);
	}

	public String getWorkersByName(String name) {
		// parse JSON
		List<String> args = Arrays.asList(
				JQlessClient.getCurrentSeconds(), name);

		return (String) this._client.call(JQlessClient.Command.WORKERS, args);
	}
}
