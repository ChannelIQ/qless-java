package com.ciq.qless.java.jobs;

import com.ciq.qless.java.client.JQlessClient;

public interface BaseJobFactory {
	BaseJob create(JQlessClient client, Attributes atts);
}
