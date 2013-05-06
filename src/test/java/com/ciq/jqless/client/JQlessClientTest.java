package com.ciq.jqless.client;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.jqless.Queue;
import com.ciq.jqless.Queue.JobOptions;
import com.ciq.jqless.jobs.Job;

public class JQlessClientTest {

	@Test
	public void testCreatingANewQueue() {
		Jedis jedis = new Jedis("localhost");
		@SuppressWarnings("unused")
		String test = jedis.get("test");

		JQlessClient client = new JQlessClient(jedis);

		Queue testQueue = client.getQueues().getNamedQueue("test_queue");

		Map<String, Object> data = new HashMap<String, Object>();
		JobOptions options = testQueue.new JobOptions();

		testQueue.put("SimpleTestJob", data, options);

		Job job = testQueue.pop();

		job.performWork();

		fail("Not yet implemented");
	}
}
