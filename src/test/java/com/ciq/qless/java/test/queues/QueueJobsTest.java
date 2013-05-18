package com.ciq.qless.java.test.queues;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.queues.QueueJobs;
import com.ciq.qless.java.test.BaseTest;

public class QueueJobsTest extends BaseTest {

	private static QueueJobs _queueJobs;

	@BeforeClass
	public static void init() {
		JedisPool pool = new JedisPool("localhost");
		_queueJobs = new QueueJobs("test-queue", new JQlessClient(pool));
	}

	@Test
	public void testQueueJobsRunning() throws LuaScriptException,
			InterruptedException {
		String jid1 = addJob();
		String jid2 = addJob();
		popJob();
		popJob();

		// Ensure that the last job doesn't come off until later
		Thread.sleep(1000);
		String jid3 = addJob();
		popJob();

		List<String> jids = _queueJobs.running(0, 2);
		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));
		assertFalse(jids.contains(jid3));

		removeJobs(jid1, jid2, jid3);
	}

	@Test
	public void testQueueJobsStalled() throws LuaScriptException,
			InterruptedException {
		// Change the heartbeat
		_queueJobs._client.setConfig("heartbeat", 5);

		String jid1 = addJob();
		String jid2 = addJob();
		popJob();
		popJob();

		// Ensure that the last job doesn't come off until later
		Thread.sleep(1000);
		String jid3 = addJob();
		popJob();

		Thread.sleep(5100);

		List<String> jids = _queueJobs.stalled(0, 2);
		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));
		assertFalse(jids.contains(jid3));

		_queueJobs._client.setConfig("heartbeat", 60);
		removeJobs(jid1, jid2, jid3);
	}

	@Test
	public void testQueueJobsScheduled() throws LuaScriptException,
			InterruptedException {
		String jid1 = addDelayedJob(60);
		String jid2 = addDelayedJob(60);
		String jid3 = addJob();

		Thread.sleep(1000);
		String jid4 = addDelayedJob(60);
		String jid5 = addJob();

		popJob();
		popJob();

		List<String> jids = _queueJobs.scheduled(0, 2);
		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));
		assertFalse(jids.contains(jid3));
		assertFalse(jids.contains(jid4));
		assertFalse(jids.contains(jid5));

		removeJobs(jid1, jid2, jid3, jid4, jid5);
	}

	@Test
	public void testQueueJobsDepends() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();
		String jid3 = addDependentJob(Arrays.asList(jid1, jid2));

		List<String> jids = _queueJobs.depends(0, 2);
		assertTrue(jids.contains(jid3));
		assertFalse(jids.contains(jid1));
		assertFalse(jids.contains(jid2));

		removeJobs(jid1, jid2, jid3);
	}

	@Test
	public void testQueueJobsRecurring() throws InterruptedException,
			LuaScriptException {
		String jid1 = addRecurringJob();
		String jid2 = addRecurringJob();

		Thread.sleep(1000);
		String jid3 = addRecurringJob();

		List<String> jids = _queueJobs.recurring(0, 2);
		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));
		assertFalse(jids.contains(jid3));

		removeJobs(jid1, jid2, jid3);
	}
}
