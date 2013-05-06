package com.ciq.jqless.jobs;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ciq.jqless.client.JQlessClient;

public abstract class RecurringJob extends BaseJob {
	private int _interval;

	public RecurringJob(JQlessClient client, Attributes atts) {
		super(client, atts);
	}

	@Override
	public abstract void performWork();

	@Override
	public void setPriority(int priority) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "priority", String.valueOf(priority));

		String retVal = (String) this._client.call(JQlessClient.Command.RECUR,
				args);

		_attributes.setPriority(priority);
	}

	public int getRetries() {
		return _attributes.getRetries();
	}

	public void setRetries(int retries) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "retries", String.valueOf(retries));

		String retVal = (String) this._client.call(JQlessClient.Command.RECUR,
				args);

		_attributes.setRetries(retries);
	}

	public int getInterval() {
		return _interval;
	}

	public void setInterval(int interval) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "interval", String.valueOf(interval));

		String retVal = (String) this._client.call(JQlessClient.Command.RECUR,
				args);

		_interval = interval;
	}

	public void setData(Map<String, Object> data) {
		// JSONify the data
		String dataJSON = "";

		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "data", dataJSON);

		this._client.call(JQlessClient.Command.RECUR, args);

		_attributes.setData(data);
	}

	public void setKlassName(String klassName) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "klass", klassName);

		String retVal = (String) this._client.call(JQlessClient.Command.RECUR,
				args);

		_attributes.setKlassName(klassName);
	}

	public void move(String newQueue) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "queue", newQueue);

		this._client.call(JQlessClient.Command.RECUR, args);

		_attributes.setQueueName(newQueue);
	}

	public void cancel() {
		List<String> args = Arrays.asList("off", _attributes.getJID()
				.toString());

		this._client.call(JQlessClient.Command.RECUR, args);
	}

	public void tag(List<String> tags) {
		List<String> args = Arrays.asList("tag", _attributes.getJID()
				.toString());
		args.addAll(tags);

		this._client.call(JQlessClient.Command.RECUR, args);
	}

	public void untag(List<String> tags) {
		List<String> args = Arrays.asList("untag", _attributes.getJID()
				.toString());
		args.addAll(tags);

		this._client.call(JQlessClient.Command.RECUR, args);
	}
}
