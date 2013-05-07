package com.ciq.qless.java.test.jobs;

import java.util.UUID;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.Attributes;
import com.ciq.qless.java.jobs.Job;

public class SimpleTestJob extends Job {

	public SimpleTestJob(JQlessClient client, Attributes attributes) {
		super(client, attributes);
	}

	@Override
	public void performWork() {
		// Do something interesting
		UUID jid = (UUID) this.getAttributes().getData().get("jid");
	}

}
