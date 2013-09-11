package com.ciq.qless.java.test.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;

import redis.clients.jedis.JedisPool;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.Attributes;
import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.test.GuiceTestRunner;
import com.ciq.qless.java.test.TestQlessJavaModule;
import com.ciq.qless.java.test.WithModules;
import com.ciq.qless.java.utils.ResponseFactory;
import com.google.inject.Inject;

@RunWith(GuiceTestRunner.class)
@WithModules({ TestQlessJavaModule.class })
public class ResponseFactoryTest {

	@Inject
	private ResponseFactory factory;

	@Test
	public void testCreateJob() {
		JedisPool pool = new JedisPool("172.16.120.180");
		JQlessClient client = new JQlessClient(pool);

		// Setup the test job
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("klass", "com.ciq.qless.java.test.jobs.SimpleTestJob");

		ClassLoader classLoader = getClass().getClassLoader();

		BaseJob createdJob = factory.createJob(classLoader, map, client);

		Attributes atts = createdJob.getAttributes();
	}
}
