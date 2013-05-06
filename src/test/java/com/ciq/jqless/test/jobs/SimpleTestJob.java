package com.ciq.jqless.test.jobs;

import java.util.UUID;

import com.ciq.jqless.client.JQlessClient;
import com.ciq.jqless.jobs.Attributes;
import com.ciq.jqless.jobs.Job;

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
