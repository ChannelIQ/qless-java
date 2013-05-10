package com.ciq.qless.java.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.utils.ResponseFactory;

public class ClientJobs {
	private final JQlessClient _client;

	public ClientJobs(JQlessClient client) {
		this._client = client;
	}

	public List<String> complete() {
		return complete(0, 25);
	}

	@SuppressWarnings("unchecked")
	public List<String> complete(int offset, int count) {
		List<String> args = Arrays.asList("complete", String.valueOf(offset),
				String.valueOf(count));

		return this._client.call(JQlessClient.Command.JOBS, args).as(
				ResponseFactory.JIDS);
	}

	public List<BaseJob> tracked() {
		return this._client.call(JQlessClient.Command.TRACK,
				new ArrayList<String>()).as(ResponseFactory.TRACKEDJOBS,
				this._client);
	}

	public List<String> tagged(String tag) {
		return tagged(tag, 0, 25);
	}

	public List<String> tagged(String tag, int offset, int count) {
		List<String> args = Arrays.asList("get", tag, String.valueOf(offset),
				String.valueOf(count));

		return this._client.call(JQlessClient.Command.TAG, args).as(
				ResponseFactory.TAGGEDJIDS);
	}

	/*
	 * Show all known failure groups
	 */
	public Map<String, Object> failed() {
		return this._client.call(JQlessClient.Command.FAILED,
				new ArrayList<String>()).as(ResponseFactory.FAILS);
	}

	/*
	 * Return the first 25 failed jobs by Group
	 */
	public List<BaseJob> failedByGroup(String tag) {
		return failedByGroup(tag, 0, 25);
	}

	/*
	 * Return failed jobs from start with a count of limit
	 */
	public List<BaseJob> failedByGroup(String tag, int start, int limit) {
		if (tag != null && tag.length() > 0) {
			List<String> args = Arrays.asList(tag, String.valueOf(start),
					String.valueOf(limit));

			return this._client.call(JQlessClient.Command.FAILED, args).as(
					ResponseFactory.FAILEDJOBS, this._client);
		} else {
			throw new IllegalArgumentException("Tag must be supplied");
		}
	}

	public BaseJob getJob(UUID id) throws QlessClientException {
		return getJob(id.toString());
	}

	public BaseJob getJob(String jid) throws QlessClientException {
		// try to get a single job
		ArrayList<String> args = new ArrayList<String>();
		args.add(jid);

		BaseJob job = this._client.call(JQlessClient.Command.GET, args).as(
				ResponseFactory.JOB, this._client);

		// TODO: CHECK THIS LOGIC
		// if fails
		if (job == null) {
			// try to get a recurring job
			args.add(0, "get");

			job = this._client.call(JQlessClient.Command.RECUR, args).as(
					ResponseFactory.RECURRINGJOB, this._client);

			if (job != null) {
				return job;
			}

			throw new QlessClientException("Job " + jid + " is unknown");
		}

		return job;
	}
}
