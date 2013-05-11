package com.ciq.qless.java.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.utils.JsonHelper;
import com.ciq.qless.java.utils.ResponseFactory;

public abstract class Job extends BaseJob {

	public Job(JQlessClient client) {
		this(client, new Attributes());
	}

	public Job(JQlessClient client, Attributes atts) {
		super(client, atts);
	}

	@Override
	public abstract void performWork();

	@Override
	public void setPriority(int priority) {
		List<String> args = Arrays.asList(_attributes.getJID(),
				String.valueOf(priority));

		boolean success = this._client
				.call(JQlessClient.Command.PRIORITY, args).as(
						ResponseFactory.BOOLEAN);

		if (success) {
			_attributes.setPriority(priority);
		}
	}

	/*
	 * Move a job to a new Queue
	 */
	@Override
	public void move(String newQueue) {
		noteStateChange("move");

		List<String> keys = Arrays.asList(newQueue);

		List<String> args = Arrays.asList(_attributes.getJID(),
				_attributes.getKlassName(),
				JsonHelper.createJSON(_attributes.getData().toString()),
				JQlessClient.getCurrentSeconds(), "0");

		String jid = this._client.call(JQlessClient.Command.PUT, keys, args)
				.as(ResponseFactory.JID);

		_attributes.setQueueName(newQueue);
	}

	/*
	 * Cancel a job
	 */
	@Override
	public void cancel() {
		noteStateChange("cancel");

		List<String> args = Arrays.asList(_attributes.getJID());

		this._client.call(JQlessClient.Command.CANCEL, args);
	}

	/*
	 * Add tags to this job
	 */
	@Override
	public List<String> tag(String... tags) {
		ArrayList<String> args = new ArrayList<String>();
		args.add("add");
		args.add(_attributes.getJID());
		args.add(JQlessClient.getCurrentSeconds());

		for (String tag : tags) {
			args.add(tag);
		}

		return this._client.call(JQlessClient.Command.TAG, args).as(
				ResponseFactory.TAGS);
	}

	/*
	 * Remove tags from this job
	 */
	@Override
	public List<String> untag(String... tags) {
		ArrayList<String> args = new ArrayList<String>();
		args.add("remove");
		args.add(_attributes.getJID());
		args.add(JQlessClient.getCurrentSeconds());

		for (String tag : tags) {
			args.add(tag);
		}

		return this._client.call(JQlessClient.Command.TAG, args).as(
				ResponseFactory.TAGS);
	}

	/*
	 * This job depends on the input jobs
	 */
	@Override
	public boolean depend(String... jids) {
		ArrayList<String> args = new ArrayList<String>();
		args.add(_attributes.getJID());
		args.add(JQlessClient.getCurrentSeconds());
		args.add("on");

		for (String jid : jids) {
			args.add(jid);
		}

		return this._client.call(JQlessClient.Command.DEPENDS, args).as(
				ResponseFactory.BOOLEAN);
	}

	/*
	 * This job no longer depends on the input jobs
	 */
	@Override
	public boolean undepend(String... jids) {
		ArrayList<String> args = new ArrayList<String>();
		args.add(_attributes.getJID());
		args.add(JQlessClient.getCurrentSeconds());
		args.add("off");

		for (String jid : jids) {
			args.add(jid);
		}

		return this._client.call(JQlessClient.Command.DEPENDS, args).as(
				ResponseFactory.BOOLEAN);
	}
}
