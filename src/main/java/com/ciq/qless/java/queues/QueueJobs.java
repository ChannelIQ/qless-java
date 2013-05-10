package com.ciq.qless.java.queues;

import java.util.Arrays;
import java.util.List;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.utils.ResponseFactory;

public class QueueJobs {
	public final String _queueName;
	public final JQlessClient _client;

	public QueueJobs(String queueName, JQlessClient client) {
		this._queueName = queueName;
		this._client = client;
	}

	public List<String> running() {
		return running(0, 25);
	}

	public List<String> running(int start, int count) {
		return callJobStatus("running", start, count);
	}

	public List<String> stalled() {
		return stalled(0, 25);
	}

	public List<String> stalled(int start, int count) {
		return callJobStatus("stalled", start, count);
	}

	public List<String> scheduled() {
		return scheduled(0, 25);
	}

	public List<String> scheduled(int start, int count) {
		return callJobStatus("scheduled", start, count);
	}

	public List<String> depends() {
		return depends(0, 25);
	}

	public List<String> depends(int start, int count) {
		return callJobStatus("depends", start, count);
	}

	public List<String> recurring() {
		return recurring(0, 25);
	}

	public List<String> recurring(int start, int count) {
		return callJobStatus("recurring", start, count);
	}

	@SuppressWarnings("unchecked")
	private List<String> callJobStatus(String status, int start, int count) {
		List<String> args = Arrays.asList(status,
				JQlessClient.getCurrentSeconds(), this._queueName,
				String.valueOf(start), String.valueOf(count));

		return this._client.call(JQlessClient.Command.JOBS, args).as(
				ResponseFactory.JIDS);
	}
}
