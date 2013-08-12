package com.ciq.qless.java.test.jobs;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.Attributes;
import com.ciq.qless.java.jobs.Job;
import com.ciq.qless.java.jobs.JobResults;

public class SimpleTestJob extends Job {

	public SimpleTestJob(JQlessClient client, Attributes attributes) {
		super(client, attributes);
	}

	public SimpleTestJob(JQlessClient _client) {
		super(_client);
	}

	@Override
	public JobResults performWork() {
		// Do something interesting
		String jid = this.getAttributes().getJID();

		try {
			complete();

			return JobResults.build().success();
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			return JobResults.build().fail().addError(ex.getMessage());
		}
	}

}
