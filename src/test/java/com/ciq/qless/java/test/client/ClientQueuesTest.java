package com.ciq.qless.java.test.client;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

import com.ciq.qless.java.client.ClientQueues;
import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.test.BaseTest;

public class ClientQueuesTest extends BaseTest {

	private static ClientQueues _clientQueues;

	@BeforeClass
	public static void init() {
		JedisPool pool = new JedisPool("localhost");
		// Jedis jedis = new Jedis("localhost");
		_clientQueues = new ClientQueues(new JQlessClient(pool));
	}

	@Test
	public void testClientQueueCounts() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();
		String jid3 = addJob(UUID.randomUUID().toString(), "another-queue");

		// Pop job to simulate worker
		popJob();

		List<Map<String, Object>> queueDetails = _clientQueues.counts();

		for (Map<String, Object> queue : queueDetails) {
			if (queue.get("name").equals(TEST_QUEUE)) {
				assertEquals("1", queue.get("running").toString());
				assertEquals("1", queue.get("waiting").toString());
			} else {
				assertEquals("0", queue.get("running").toString());
				assertEquals("1", queue.get("waiting").toString());
			}
			assertEquals("0", queue.get("scheduled").toString());
		}

		removeJob(jid1);
		removeJob(jid2);
		removeJob(jid3);
	}

	@Test
	public void testClientQueueGetByName() {
		Queue q = testQueue();

		Queue queue = _clientQueues.getNamedQueue(TEST_QUEUE);

		assertEquals(q.name(), queue.name());
		assertEquals(q.getHeartbeat(), queue.getHeartbeat());
	}

	private Queue testQueue() {
		return new Queue(TEST_QUEUE, _clientQueues._client, TEST_WORKER);
	}
}
