package com.ciq.qless.java;

import java.util.Arrays;
import java.util.List;

import com.ciq.qless.java.client.JQlessClient;

public class Config {
	private final JQlessClient _client;

	public Config(JQlessClient client) {
		this._client = client;
	}

	public String get(String key) {
		List<String> args = Arrays.asList("get", key);

		return (String) this._client.call(JQlessClient.Command.CONFIG, args);
	}

	public void set(String key, Object value) {
		List<String> args = Arrays.asList("set", key, String.valueOf(value));

		this._client.call(JQlessClient.Command.CONFIG, args);
	}

	@SuppressWarnings("unchecked")
	public List<String> all() {
		List<String> args = Arrays.asList("get");

		return (List<String>) this._client.call(JQlessClient.Command.CONFIG,
				args);
	}

	public void clear(String key) {
		List<String> args = Arrays.asList("unset", key);

		this._client.call(JQlessClient.Command.CONFIG, args);
	}
}
