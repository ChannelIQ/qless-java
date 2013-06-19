package com.ciq.qless.java.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.utils.ResponseFactory;

public abstract class RecurringJob extends BaseJob {
	public RecurringJob(JQlessClient client, Attributes atts) {
		super(client, atts);
	}

	@Override
	public abstract boolean performWork() throws Exception;

	@Override
	public void setPriority(int priority) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "priority", String.valueOf(priority));

		boolean success = this._client.call(JQlessClient.Command.RECUR, args)
				.as(ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setPriority(priority);
		}
	}

	public int getRetries() {
		return _attributes.getRetries();
	}

	public void setRetries(int retries) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "retries", String.valueOf(retries));

		boolean success = this._client.call(JQlessClient.Command.RECUR, args)
				.as(ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setRetries(retries);
		}
	}

	public int getInterval() {
		return _attributes.getInterval();
	}

	public void setInterval(int interval) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "interval", String.valueOf(interval));

		boolean success = this._client.call(JQlessClient.Command.RECUR, args)
				.as(ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setInterval(interval);
		}
	}

	public void setData(Map<String, Object> data) {
		// JSONify the data
		String dataJSON = "";

		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "data", dataJSON);

		boolean success = this._client.call(JQlessClient.Command.RECUR, args)
				.as(ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setData(data);
		}
	}

	public void setKlassName(String klassName) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "klass", klassName);

		boolean success = this._client.call(JQlessClient.Command.RECUR, args)
				.as(ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setKlassName(klassName);
		}
	}

	@Override
	public void move(String newQueue) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "queue", newQueue);

		boolean success = this._client.call(JQlessClient.Command.RECUR, args)
				.as(ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setQueueName(newQueue);
		}
	}

	@Override
	public void cancel() {
		List<String> args = Arrays.asList("off", _attributes.getJID()
				.toString());

		this._client.call(JQlessClient.Command.RECUR, args);
	}

	@Override
	public List<String> tag(String... tags) {
		ArrayList<String> args = new ArrayList<String>();
		args.add("tag");
		args.add(_attributes.getJID());
		for (String tag : tags) {
			args.add(tag);
		}

		return this._client.call(JQlessClient.Command.RECUR, args).as(
				ResponseFactory.TAGS);
	}

	@Override
	public List<String> untag(String... tags) {
		ArrayList<String> args = new ArrayList<String>();
		args.add("untag");
		args.add(_attributes.getJID());
		for (String tag : tags) {
			args.add(tag);
		}

		return this._client.call(JQlessClient.Command.RECUR, args).as(
				ResponseFactory.TAGS);
	}
}
