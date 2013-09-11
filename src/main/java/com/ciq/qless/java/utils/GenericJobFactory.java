package com.ciq.qless.java.utils;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.Attributes;
import com.ciq.qless.java.jobs.BaseJob;

public interface GenericJobFactory<T extends BaseJob> {
	public T create(JQlessClient client, Attributes atts);
}
