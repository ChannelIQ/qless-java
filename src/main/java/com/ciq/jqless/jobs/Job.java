package com.ciq.jqless.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.joda.time.LocalDateTime;

import com.ciq.jqless.client.JQlessClient;

public abstract class Job extends BaseJob {
	private final Attributes _attributes;
	private boolean _stateChanged;

	public Job(JQlessClient client) {
		this(client, new Attributes());
	}

	public Job(JQlessClient client, Attributes atts) {
		super(client, atts);

		_attributes = atts;
		_stateChanged = false;
	}

	public String getDescription() {
		return String.format("%s (%s / %s / $s)", _attributes.getKlassName(),
				_attributes.getJID(), _attributes.getQueueName(),
				_attributes.getState());
	}

	public boolean isStateChanged() {
		return _stateChanged;
	}

	public long getTimeToLive() {
		return _attributes.getExpiresAt().toDateTime().getMillis()
				- LocalDateTime.now().toDateTime().getMillis();
	}

	@Override
	public void setPriority(int priority) {
		List<String> args = Arrays.asList(_attributes.getJID().toString(),
				String.valueOf(priority));

		String retVal = (String) this._client.call(
				JQlessClient.Command.PRIORITY, args);

		_attributes.setPriority(priority);
	}

	public void getQueueHistory() {
		// Fill our QueueHistory hash with values form the raw queue history
		// stuffs
	}

	public LocalDateTime getFirstOccurence() {
		// TODO: Search the history for the minimum timestamp
		return LocalDateTime.now();
	}

	public Map<String, Object> getHash() {
		return _attributes.getHash();
	}

	/*
	 * Move a job to a new Queue
	 */
	public void move(String newQueue) {
		noteStateChange("move");

		List<String> keys = Arrays.asList(newQueue);

		// TODO: Add JSON around Data
		List<String> args = Arrays.asList(_attributes.getJID().toString(),
				_attributes.getKlassName(), _attributes.getData().toString(),
				JQlessClient.getCurrentSeconds(), "0");

		UUID jid = (UUID) this._client.call(JQlessClient.Command.PUT, keys,
				args);

		_attributes.setQueueName(newQueue);
	}

	/*
	 * Fail a job
	 */
	public void fail(String group, String message) {
		noteStateChange("fail");

		// TODO: Add JSON around Data
		List<String> args = Arrays.asList(_attributes.getJID().toString(),
				_attributes.getWorkerName(), group, message, JQlessClient
						.getCurrentSeconds(), _attributes.getData()
						.toString());

		// TODO: Catch failures (returns JID or False)
		this._client.call(JQlessClient.Command.FAIL, args);
	}

	/*
	 * Send heartbeat to the job
	 */
	public void heartbeat() {
		List<String> args = Arrays.asList(_attributes.getJID().toString(),
				_attributes.getWorkerName(), JQlessClient
						.getCurrentSeconds(), _attributes.getData()
						.toString());

		// TODO: Catch return type (Timestamp or False)
		this._client.call(JQlessClient.Command.HEARTBEAT, args);
	}

	/*
	 * Complete the job
	 */
	public String complete() throws Exception {
		return complete("", 0);
	}

	public String complete(String nextQueue, int delay) throws Exception {
		return complete(nextQueue, delay, new ArrayList<Object>());
	}

	public String complete(String nextQueue, List<Object> depends)
			throws Exception {
		return complete(nextQueue, 0, depends);
	}

	public String complete(String nextQueue, int delay, List<Object> depends)
			throws Exception {
		noteStateChange("complete");
		String newStatus = "";

		// TODO: JSonify the Data object
		List<String> args = Arrays.asList(_attributes.getJID().toString(),
				_attributes.getWorkerName(), _attributes.getQueueName(),
				JQlessClient.getCurrentSeconds(), _attributes.getData()
						.toString());

		if (nextQueue != null && nextQueue.length() > 0) {
			args.add("next");
			args.add(nextQueue);
			args.add("delay");
			args.add(String.valueOf(delay));
			args.add("depends");
			// TODO: JSOnify this List
			args.add(depends.toString());
		}

		newStatus = (String) this._client.call(JQlessClient.Command.COMPLETE,
				args);

		if (newStatus != null && newStatus.length() > 0) {
			return newStatus;
		} else {
			Job reloadedJob = _client.getJobs().getJob(_attributes.getJID());
			String description = "";
			if (reloadedJob != null) {
				description = reloadedJob.getDescription();
			} else {
				description = getDescription() + " -- can't be reloaded";
			}

			throw new Exception("Failed to complete - " + description);
		}
	}

	/*
	 * Cancel a job
	 */
	public void cancel() {
		noteStateChange("cancel");

		List<String> args = Arrays.asList(_attributes.getJID().toString());

		this._client.call(JQlessClient.Command.CANCEL, args);
	}

	/*
	 * Track this job
	 */
	public void track() {
		List<String> args = Arrays.asList("track", _attributes.getJID()
				.toString(), JQlessClient.getCurrentSeconds());

		this._client.call(JQlessClient.Command.TRACK, args);
	}

	/*
	 * Untrack this job
	 */
	public void untrack() {
		List<String> args = Arrays.asList("untrack", _attributes.getJID()
				.toString(), JQlessClient.getCurrentSeconds());

		this._client.call(JQlessClient.Command.TRACK, args);
	}

	/*
	 * Add tags to this job
	 */
	public void tag(List<String> tags) {
		List<String> args = Arrays.asList("add", _attributes.getJID()
				.toString(), JQlessClient.getCurrentSeconds());

		args.addAll(tags);

		this._client.call(JQlessClient.Command.TAG, args);
	}

	/*
	 * Remove tags from this job
	 */
	public void untag(List<String> tags) {
		List<String> args = Arrays.asList("remove", _attributes.getJID()
				.toString(), JQlessClient.getCurrentSeconds());

		args.addAll(tags);

		this._client.call(JQlessClient.Command.TAG, args);
	}

	/*
	 * Retry with a delay of 0
	 */
	public int retry() {
		return retry(0);
	}

	/*
	 * Retry with a custom delay
	 */
	public int retry(int delay) {
		noteStateChange("retry");

		List<String> args = Arrays.asList(_attributes.getJID().toString(),
				_attributes.getQueueName(), _attributes.getWorkerName(),
				JQlessClient.getCurrentSeconds(), String.valueOf(delay));

		Object retriesRemaining = this._client.call(JQlessClient.Command.RETRY,
				args);

		if (retriesRemaining instanceof Integer) {
			return (Integer) retriesRemaining;
		}

		return 0;
	}

	/*
	 * This job depends on the input jobs
	 */
	public boolean depend(List<String> jids) {
		List<String> args = Arrays
				.asList(_attributes.getJID().toString(), "on");

		args.addAll(jids);

		return (Boolean) this._client.call(JQlessClient.Command.DEPENDS, args);
	}

	/*
	 * This job no longer depends on the input jobs
	 */
	public boolean undepend(List<String> jids) {
		List<String> args = Arrays.asList(_attributes.getJID().toString(),
				"off");

		args.addAll(jids);

		return (Boolean) this._client.call(JQlessClient.Command.DEPENDS, args);
	}

	private Object noteStateChange(String eventType) {
		// Call before_callback
		// get results
		_stateChanged = false;
		// Call after_callback
		return null;
	}

	@Override
	public abstract void performWork();
}
