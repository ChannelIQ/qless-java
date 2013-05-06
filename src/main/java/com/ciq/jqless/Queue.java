package com.ciq.jqless;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.ciq.jqless.client.JQlessClient;
import com.ciq.jqless.jobs.Job;

public class Queue {
	public final String _queueName;
	public final JQlessClient _client;
	public final String _workerName;
	public QueueJobs _jobs = null;

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

	public int count() {
		// parse JSON
		List<String> args = Arrays.asList(
				JQlessClient.getCurrentSeconds(), this._queueName);

		return Integer.valueOf(String.valueOf(this._client.call(
				JQlessClient.Command.QUEUES, args)));
	}

	public void pause() {
		List<String> args = Arrays.asList(this._queueName);

		this._client.call(JQlessClient.Command.PAUSE, args);
	}

	public void unpause() {
		List<String> args = Arrays.asList(this._queueName);

		this._client.call(JQlessClient.Command.UNPAUSE, args);
	}

	public void getHeartbeat() {
		this._client.getConfig(this._queueName + "-heartbeat");
	}

	public void setHeartbeat(int interval) {
		this._client.setConfig(this._queueName + "-heartbeat", interval);
	}

	public String generateJSON(Object obj) {
		return "";
	}

	/*
	 * Put the described job in this queue
	 */
	public void put(String klass, Map<String, Object> data, JobOptions options) {
		options = defaultJobOptions(klass, data, options);

		List<String> keys = Arrays.asList(this._queueName);

		List<String> args = Arrays.asList(options.getJID().toString(), klass,
				generateJSON(data), JQlessClient.getCurrentSeconds(),
				String.valueOf(options.getDelay()), "priority",
				String.valueOf(options.getPriority()), "tag",
				generateJSON(options.getTags()), "retries",
				String.valueOf(options.getRetries()), "depends",
				generateJSON(options.getDepends()));

		this._client.call(JQlessClient.Command.PUT, keys, args);
	}

	/*
	 * Set the described job to be recurring in this queue
	 */
	public void recur(String klass, Map<String, Object> data, int interval,
			JobOptions options) {
		options = defaultJobOptions(klass, data, options);

		List<String> args = Arrays.asList("on", this._queueName, options
				.getJID().toString(), klass, generateJSON(data), JQlessClient
				.getCurrentSeconds(), "interval",
				String.valueOf(interval), String.valueOf(options.getOffset()),
				"priority", String.valueOf(options.getPriority()), "tags",
				generateJSON(options.getTags()), "retries",
				generateJSON(options.getDepends()));

		this._client.call(JQlessClient.Command.RECUR, args);
	}

	/*
	 * Pop a single item off the Queue
	 */
	public Job pop() {
		List<Job> jobs = pop(1);
		if (jobs != null && jobs.size() > 0)
			return jobs.get(0);

		return null;
	}

	/*
	 * Pop any number of items off the Queue
	 */
	@SuppressWarnings("unchecked")
	public List<Job> pop(int count) {
		List<String> args = Arrays.asList(String.valueOf(this._workerName),
				String.valueOf(count), JQlessClient.getCurrentSeconds());

		return queryJob(JQlessClient.Command.POP, args);
	}

	/*
	 * Peek at a single work item in the Queue
	 */
	public Job peek() {
		List<Job> jobs = peek(1);
		if (jobs != null && jobs.size() > 0)
			return jobs.get(0);

		return null;
	}

	/*
	 * Peek at a set of work items from the Queue
	 */
	public List<Job> peek(int count) {
		List<String> args = Arrays.asList(String.valueOf(count),
				JQlessClient.getCurrentSeconds());

		return queryJob(JQlessClient.Command.PEEK, args);
	}

	private List<Job> queryJob(JQlessClient.Command command, List<String> args) {
		List<String> keys = Arrays.asList(this._queueName);

		Object jobsJSON = this._client.call(command, keys, args);

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> jobs2 = (List<Map<String, Object>>) jobsJSON;
		List<Job> jobs = new ArrayList<Job>();

		for (Map<String, Object> jobJSON : jobs2) {
			// Job job = new Job(this._client, new Attributes(jobJSON));
			// jobs.add(job);
		}

		return jobs;
	}

	public String stats() {
		return stats(JQlessClient.getCurrentSeconds());
	}

	public String stats(String timestamp) {
		List<String> args = Arrays.asList(this._queueName, timestamp);

		return (String) this._client.call(JQlessClient.Command.STATS, args);
	}

	public int length() {
		return this._client.getQueueLength(this._queueName);
	}

	private JobOptions defaultJobOptions(String klass,
			Map<String, Object> data, JobOptions options) {

		return options;
	}

	public class JobOptions {
		public UUID _jid = UUID.randomUUID();
		public List<String> _depends = new ArrayList<String>();
		public List<String> _tags = new ArrayList<String>();
		public int _delay = 0;
		public int _priority = 0;
		public int _offset = 0;
		public int _retries = 5;

		public UUID getJID() {
			return this._jid;
		}

		public List<String> getDepends() {
			return this._depends;
		}

		public int getPriority() {
			return this._priority;
		}

		public List<String> getTags() {
			return this._tags;
		}

		public int getDelay() {
			return this._delay;
		}

		public int getOffset() {
			return this._offset;
		}

		public int getRetries() {
			return this._retries;
		}

	}
}
