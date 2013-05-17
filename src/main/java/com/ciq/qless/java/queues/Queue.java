package com.ciq.qless.java.queues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.jobs.JobOptions;
import com.ciq.qless.java.utils.JsonHelper;
import com.ciq.qless.java.utils.ResponseFactory;

public class Queue {
	private final String _queueName;
	private final JQlessClient _client;
	private final String _workerName;
	private QueueJobs _jobs = null;

	public Queue(String queueName, JQlessClient client, String workerName) {
		this._queueName = queueName;
		this._client = client;
		this._workerName = workerName;
	}

	public QueueJobs jobs() {
		if (_jobs == null)
			_jobs = new QueueJobs(this._queueName, this._client);

		return _jobs;
	}

	public String name() {
		return _queueName;
	}

	public Map<String, Object> count() {
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds(),
				this._queueName);

		return this._client.call(JQlessClient.Command.QUEUES, args).as(
				ResponseFactory.QUEUE);
	}

	public void pause() {
		List<String> args = Arrays.asList(this._queueName);

		this._client.call(JQlessClient.Command.PAUSE, args);
	}

	public void unpause() {
		List<String> args = Arrays.asList(this._queueName);

		this._client.call(JQlessClient.Command.UNPAUSE, args);
	}

	public String getHeartbeat() {
		return this._client.getConfig(this._queueName + "-heartbeat");
	}

	public void setHeartbeat(int interval) {
		this._client.setConfig(this._queueName + "-heartbeat", interval);
	}

	/*
	 * Put the described job in this queue
	 */
	public void put(BaseJob job) {
		put(job, job.getDefaultOptions());
	}

	public void put(BaseJob job, JobOptions options) {
		put(job, options, job.getAttributes().getData());
	}

	public void put(BaseJob job, JobOptions options, Map<String, Object> data) {
		put(job.getClass().getName(), options, data);
	}

	public void put(Class<?> klass, JobOptions options, Map<String, Object> data) {
		put(klass.getName(), options, data);
	}

	public void put(String klass, JobOptions options, Map<String, Object> data) {
		List<String> keys = Arrays.asList(this._queueName);

		ArrayList<String> args = new ArrayList<String>();
		args.add(options.getJID());
		args.add(klass);

		// Override data field if filled
		if (options.getData().size() != 0) {
			data = options.getData();
		}

		args.add(JsonHelper.createJSON(data));
		args.add(JQlessClient.getCurrentSeconds());
		args.add(String.valueOf(options.getDelay()));

		if (options.getPriority() != 0) {
			args.add("priority");
			args.add(String.valueOf(options.getPriority()));
		}

		if (options.getTags().size() > 0) {
			args.add("tags");
			args.add(JsonHelper.createJSON(options.getTags()));
		}

		if (options.getRetries() != 5) {
			args.add("retries");
			args.add(String.valueOf(options.getRetries()));
		}

		if (options.getDepends().size() != 0) {
			args.add("depends");
			args.add(JsonHelper.createJSON(options.getDepends()));
		}

		this._client.call(JQlessClient.Command.PUT, keys, args);
	}

	/*
	 * Set the described job to be recurring in this queue
	 */
	public void recur(BaseJob job, int interval) {
		recur(job, interval, job.getDefaultOptions());
	}

	public void recur(BaseJob job, int interval, JobOptions options) {
		recur(job.getClass().getName(), job.getAttributes().getData(),
				interval, options);
	}

	public void recur(String klass, Map<String, Object> data, int interval,
			JobOptions options) {

		ArrayList<String> args = new ArrayList<String>();
		args.add("on");
		args.add(this._queueName);
		args.add(options.getJID());
		args.add(klass);

		// Override data field if filled
		if (options.getData().size() != 0) {
			data = options.getData();
		}

		args.add(JsonHelper.createJSON(data));
		args.add(JQlessClient.getCurrentSeconds());
		args.add("interval");
		args.add(String.valueOf(interval));
		args.add(String.valueOf(options.getOffset()));

		if (options.getPriority() != 0) {
			args.add("priority");
			args.add(String.valueOf(options.getPriority()));
		}

		if (options.getTags().size() > 0) {
			args.add("tags");
			args.add(JsonHelper.createJSON(options.getTags()));
		}

		if (options.getRetries() != 5) {
			args.add("retries");
			args.add(String.valueOf(options.getRetries()));
		}

		this._client.call(JQlessClient.Command.RECUR, args);
	}

	/*
	 * Pop a single item off the Queue
	 */
	public BaseJob pop() {
		List<BaseJob> jobs = pop(1);
		if (jobs != null && jobs.size() > 0)
			return jobs.get(0);

		return null;
	}

	/*
	 * Pop any number of items off the Queue
	 */
	@SuppressWarnings("unchecked")
	public List<BaseJob> pop(int count) {
		List<String> args = Arrays.asList(String.valueOf(this._workerName),
				String.valueOf(count), JQlessClient.getCurrentSeconds());

		return queryJob(JQlessClient.Command.POP, args);
	}

	/*
	 * Peek at a single work item in the Queue
	 */
	public BaseJob peek() {
		List<BaseJob> jobs = peek(1);
		if (jobs != null && jobs.size() > 0)
			return jobs.get(0);

		return null;
	}

	/*
	 * Peek at a set of work items from the Queue
	 */
	public List<BaseJob> peek(int count) {
		List<String> args = Arrays.asList(String.valueOf(count),
				JQlessClient.getCurrentSeconds());

		return queryJob(JQlessClient.Command.PEEK, args);
	}

	private List<BaseJob> queryJob(JQlessClient.Command command,
			List<String> args) {
		List<String> keys = Arrays.asList(this._queueName);

		return this._client.call(command, keys, args).as(ResponseFactory.JOBS,
				this._client);
	}

	public Map<String, Object> stats() {
		return stats(JQlessClient.getCurrentSeconds());
	}

	public Map<String, Object> stats(String timestamp) {
		List<String> args = Arrays.asList(this._queueName, timestamp);

		return this._client.call(JQlessClient.Command.STATS, args).as(
				ResponseFactory.STATS);
	}

	public int length() {
		return this._client.getQueueLength(this._queueName);
	}
}
