package com.ciq.qless.java.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.client.QlessClientException;
import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.utils.JsonHelper;
import com.ciq.qless.java.utils.ResponseFactory;

public abstract class BaseJob {
	protected final JQlessClient _client;
	protected Attributes _attributes;
	protected Map<String, Object> _queueHistory = null;
	private boolean _stateChanged;

	private Queue _queue = null;

	public BaseJob(JQlessClient client, Attributes atts) {
		this._client = client;
		this._attributes = atts;
		_stateChanged = false;
	};

	public abstract boolean performWork() throws Exception;

	public JQlessClient getClient() {
		return _client;
	}

	public Attributes getAttributes() {
		return _attributes;
	}

	public String getDescription() {
		if (_attributes != null) {
			return String.format("%s (%s / %s / %s)",
					_attributes.getKlassName(), _attributes.getJID(),
					_attributes.getQueueName(), _attributes.getState());
		} else {
			return "Job Not initialized";
		}
	}

	public boolean isStateChanged() {
		return _stateChanged;
	}

	public Queue getQueue() {
		if (_queue == null)
			_queue = new Queue(_attributes.getQueueName(), _client,
					JQlessClient.getMachineName());

		return _queue;
	}

	public long getTimeToLive() {
		return (_attributes.getExpiresAt().toDateTime().getMillis() / 1000)
				- (LocalDateTime.now().toDateTime().getMillis() / 1000);
	}

	public void setPriority(int priority) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "priority", String.valueOf(priority));

		boolean success = this._client.call(JQlessClient.Command.RECUR, args)
				.as(ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setPriority(priority);
		}
	}

	public List<Map<String, Object>> getQueueHistory() {
		return _attributes.getHistory();
	}

	public LocalDateTime getFirstOccurence() throws QlessClientException {
		if (_attributes != null && _attributes.getHistory() != null) {
			int lowestTime = Integer.MAX_VALUE;
			for (Map<String, Object> jobHistory : _attributes.getHistory()) {
				if (jobHistory.containsKey("put")) {
					if (Integer.valueOf(jobHistory.get("put").toString()) < lowestTime) {
						lowestTime = Integer.valueOf(jobHistory.get("put")
								.toString());
					}
				}
			}

			return new LocalDateTime(lowestTime * 1000, DateTimeZone.UTC);
		}
		// TODO: Search the history for the minimum timestamp
		throw new QlessClientException("This job wasn't found: "
				+ this.getAttributes().getJID());
	}

	public Map<String, Object> getHash() {
		return _attributes.getHash();
	}

	public Object getDataField(String field) {
		return _attributes.getData().get(field);
	}

	public void setDataField(String field, Object value) {
		Map<String, Object> data = _attributes.getData();

		data.put(field, value);

		_attributes.setData(data);
	}

	public JobOptions getDefaultOptions() {
		String jid = UUID.randomUUID().toString();

		if (_attributes != null) {
			jid = _attributes.getJID();
		}

		return new JobOptions.OptionsBuilder(jid).build();
	}

	protected Object noteStateChange(String eventType) {
		// Call before_callback
		// get results
		_stateChanged = true;
		// Call after_callback
		return null;
	}

	/*
	 * Move a job to a new Queue
	 */
	public abstract void move(String newQueue);

	/*
	 * Fail a job
	 */
	public String fail(String group, String message) {
		noteStateChange("fail");

		List<String> args = Arrays.asList(_attributes.getJID(),
				_attributes.getWorkerName(), group, message,
				JQlessClient.getCurrentSeconds(),
				JsonHelper.createJSON(_attributes.getData().toString()));

		return this._client.call(JQlessClient.Command.FAIL, args).as(
				ResponseFactory.JID);
	}

	/*
	 * Send heartbeat to the job
	 */
	public long heartbeat() {
		List<String> args = Arrays.asList(_attributes.getJID(),
				_attributes.getWorkerName(), JQlessClient.getCurrentSeconds(),
				_attributes.getData().toString());

		return this._client.call(JQlessClient.Command.HEARTBEAT, args).as(
				ResponseFactory.HEARTBEAT);
	}

	/*
	 * Complete the job
	 */
	public String complete() throws QlessClientException {
		return complete("", 0);
	}

	public String complete(String nextQueue, int delay)
			throws QlessClientException {
		return complete(nextQueue, delay, new ArrayList<Object>());
	}

	public String complete(String nextQueue, List<Object> depends)
			throws QlessClientException {
		return complete(nextQueue, 0, depends);
	}

	public String complete(String nextQueue, int delay, List<Object> depends)
			throws QlessClientException {
		noteStateChange("complete");
		String newStatus = "";

		ArrayList<String> args = new ArrayList<String>();
		args.add(_attributes.getJID());
		args.add(_attributes.getWorkerName());
		args.add(_attributes.getQueueName());
		args.add(JQlessClient.getCurrentSeconds());
		args.add(JsonHelper.createJSON(_attributes.getData().toString()));

		if (nextQueue != null && nextQueue.length() > 0) {
			args.add("next");
			args.add(nextQueue);
			if (delay != 0) {
				args.add("delay");
				args.add(String.valueOf(delay));
			}
			if (depends.size() > 0) {
				args.add("depends");
				args.add(JsonHelper.createJSON(depends.toString()));
			}
		}

		newStatus = this._client.call(JQlessClient.Command.COMPLETE, args).as(
				ResponseFactory.STATUS);

		if (newStatus != null && newStatus.length() > 0) {
			return newStatus;
		} else {
			BaseJob reloadedJob = _client.Jobs().getJob(_attributes.getJID());
			String description = "";
			if (reloadedJob != null) {
				description = reloadedJob.getDescription();
				return description;
			} else {
				description = getDescription() + " -- can't be reloaded";
				throw new QlessClientException("Failed to complete - "
						+ description);
			}
		}
	}

	/*
	 * Track this job
	 */
	public boolean track() {
		List<String> args = Arrays.asList("track", _attributes.getJID()
				.toString(), JQlessClient.getCurrentSeconds());

		return this._client.call(JQlessClient.Command.TRACK, args).as(
				ResponseFactory.BOOLEAN);
	}

	/*
	 * Untrack this job
	 */
	public boolean untrack() {
		List<String> args = Arrays.asList("untrack", _attributes.getJID()
				.toString(), JQlessClient.getCurrentSeconds());

		return this._client.call(JQlessClient.Command.TRACK, args).as(
				ResponseFactory.BOOLEAN);
	}

	/*
	 * Add tags to this job
	 */
	public abstract List<String> tag(String... tags);

	/*
	 * Remove tags from this job
	 */
	public abstract List<String> untag(String... tags);

	/*
	 * Cancel a job
	 */
	public abstract void cancel();

	/*
	 * Retry immediately
	 */
	public long retry() {
		return retry(0);
	}

	/*
	 * Retry with a custom delay
	 */
	public long retry(int delay) {
		noteStateChange("retry");

		List<String> args = Arrays.asList(_attributes.getJID(),
				_attributes.getQueueName(), _attributes.getWorkerName(),
				JQlessClient.getCurrentSeconds(), String.valueOf(delay));

		return this._client.call(JQlessClient.Command.RETRY, args).as(
				ResponseFactory.RETRIES);
	}

	/*
	 * This job depends on the input jobs
	 */
	public abstract boolean depend(String... jids);

	/*
	 * This job no longer depends on the input jobs
	 */
	public abstract boolean undepend(String... jids);
}
