package com.ciq.qless.java.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ciq.qless.java.utils.ResponseFactory;

public class Config {
	public final JQlessClient _client;
	public static final String APPLICATION = "application";
	public static final String APPLICATION_DEFAULT = "qless";
	public static final String HEARTBEAT = "heartbeat";
	public static final String HEARTBEAT_DEFAULT = "60";
	public static final String STATS_HISTORY = "stats-history";
	public static final String STATS_HISTORY_DEFAULT = "30";
	public static final String HISTOGRAM_HISTORY = "histogram-history";
	public static final String HISTOGRAM_HISTORY_DEFAULT = "7";
	public static final String JOB_HISTORY_COUNT = "jobs-history-count";
	public static final String JOB_HISTORY_COUNT_DEFAULT = "50000";
	public static final String JOBS_HISTORY = "jobs-history";
	public static final String JOBS_HISTORY_DEFAULT = "604800";

	public Config(JQlessClient client) {
		this._client = client;
	}

	public String get(String key) {
		List<String> args = Arrays.asList("get", key);

		return this._client.call(JQlessClient.Command.CONFIG, args).as(
				ResponseFactory.CONFIG);
	}

	public void set(String key, Object value) {
		List<String> args = Arrays.asList("set", key, String.valueOf(value));

		this._client.call(JQlessClient.Command.CONFIG, args);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> all() {
		List<String> args = Arrays.asList("get");

		return this._client.call(JQlessClient.Command.CONFIG, args).as(
				ResponseFactory.CONFIGS);
	}

	public void clear(String key) {
		List<String> args = Arrays.asList("unset", key);

		this._client.call(JQlessClient.Command.CONFIG, args);
	}
}
