package com.ciq.jqless.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.ciq.jqless.jobs.Job;

public class ClientJobs {
	private final JQlessClient _client;

	public ClientJobs(JQlessClient client) {
		this._client = client;
	}

	public List<Object> complete() {
		return complete(0, 25);
	}

	@SuppressWarnings("unchecked")
	public List<Object> complete(int offset, int count) {
		List<String> args = Arrays.asList("complete", String.valueOf(offset),
				String.valueOf(count));

		return (List<Object>) this._client.call(JQlessClient.Command.COMPLETE,
				args);
	}

	public List<Job> tracked() {
		// JSON parsing
		String jobJSON = (String) this._client.call(JQlessClient.Command.TRACK,
				new ArrayList<String>());

		// Find results listed with 'jobs'

		// Create new Jobs from JSON

		List<Job> jobs = new ArrayList<Job>();
		return jobs;
	}

	public void tagged(String tag) {
		tagged(tag, 0, 25);
	}

	public void tagged(String tag, int offset, int count) {
		// Parse the JSON
		List<String> args = Arrays.asList("get", tag, String.valueOf(offset),
				String.valueOf(count));

		String json = (String) this._client
				.call(JQlessClient.Command.TAG, args);
	}

	/*
	 * Return the first 25 failed jobs
	 */
	public List<Job> failed() {
		return failed("");
	}

	public List<Job> failed(String tag) {
		return failed(tag, 0, 25);
	}

	/*
	 * Return failed jobs from start with a count of limit
	 */
	public List<Job> failed(String tag, int start, int limit) {
		if (tag != null && tag.length() > 0) {
			// JSON me
			List<String> args = Arrays.asList(tag, String.valueOf(start),
					String.valueOf(limit));

			String jobsJSON = (String) this._client.call(
					JQlessClient.Command.FAILED, args);

			// Pull all the jobs out and create new Jobs via Job Builder

			List<Job> jobs = new ArrayList<Job>();

			return jobs;

		} else {
			// JSON me
			String jobsJSON = (String) this._client.call(
					JQlessClient.Command.FAILED, new ArrayList<String>());

			List<Job> jobs = new ArrayList<Job>();

			return jobs;
		}

	}

	public Job getJob(UUID id) {
		// try to get a single job
		List<String> args = Arrays.asList(id.toString());

		String results = (String) this._client.call(JQlessClient.Command.GET,
				args);

		// if fails
		if (results == null || results.length() == 0) {
			// try to get a recurring job
			args.add(0, "get");

			results = (String) this._client.call(JQlessClient.Command.RECUR,
					args);

			// if fails return null;
			if (results == null || results.length() == 0) {
				return null;
			}

			// otherwise return new RecurringJob(this._client,
			// JSON.parse(results));

		}

		// otherwise return new Job(this._client, JSON.parse(results));

		return null;
	}

}
