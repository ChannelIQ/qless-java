package com.ciq.qless.java.test.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.jobs.JobOptions;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.test.BaseTest;
import com.ciq.qless.java.test.jobs.SimpleTestJob;

public class QueueTest extends BaseTest {

	private static Queue _queue;

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		_queue = new Queue("test-queue", new JQlessClient(jedis), "test-worker");
	}

	@Test
	public void testQueueGetJobs() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();

		popJob();
		popJob();

		List<String> jids = _queue.jobs().running();
		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));

		removeJobs(jid1, jid2);
	}

	@Test
	public void testGetQueueName() {
		assertEquals("test-queue", _queue.name());
	}

	@Test
	public void testQueueGetCount() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();
		popJob();
		popJob();
		String jid3 = addDelayedJob(60);
		String jid4 = addDependentJob(Arrays.asList(jid1, jid2));
		String jid5 = addRecurringJob();
		String jid6 = addRecurringJob();

		Map<String, Object> counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(2, counts.get("running"));
		assertEquals(1, counts.get("scheduled"));
		assertEquals(1, counts.get("depends"));
		assertEquals(2, counts.get("recurring"));

		removeJobs(jid1, jid2, jid3, jid4, jid5, jid6);
	}

	@Test
	public void testQueuePause() throws LuaScriptException {
		String jid1 = addJob();

		_queue.pause();

		popJob();

		// Pop should not effect the job, since the queue is paused
		Map<String, Object> counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(0, counts.get("running"));

		removeJobs(jid1);
	}

	@Test
	public void testQueueUnpause() throws LuaScriptException {
		String jid1 = addJob();

		_queue.pause();

		popJob();

		// Pop should not effect the job, since the queue is paused
		Map<String, Object> counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(0, counts.get("running"));

		_queue.unpause();

		popJob();

		counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(1, counts.get("running"));

		removeJobs(jid1);
	}

	@Test
	public void testQueueGetSetHeartbeat() {
		String hb = _queue.getHeartbeat();
		assertEquals("", hb);

		_queue.setHeartbeat(65);

		hb = _queue.getHeartbeat();
		assertEquals("65", hb);
	}

	@Test
	public void testQueuePutNewJob() throws LuaScriptException {
		JobOptions options = new JobOptions.OptionsBuilder(TEST_JID).build();

		Map<String, Object> data = new HashMap<String, Object>();
		data.put("url", "http://www.homepage.com");
		_queue.put(SimpleTestJob.class.getName(), data, options);

		BaseJob job = _queue.pop();

		assertEquals(TEST_JID, job.getAttributes().getJID());
		assertTrue(job.getAttributes().getData().containsKey("url"));
		assertTrue(job.getAttributes().getData().get("url")
				.equals("http://www.homepage.com"));

		removeJobs(TEST_JID);
	}

	@Test
	public void testQueuePutNewRecurringJob() throws Exception {
		JobOptions options = new JobOptions.OptionsBuilder(TEST_JID).build();

		Map<String, Object> data = new HashMap<String, Object>();
		data.put("url", "http://www.homepage.com");
		_queue.recur(SimpleTestJob.class.getName(), data, 1, options);

		BaseJob job = _queue.pop();
		Thread.sleep(2000);
		job.complete();

		Thread.sleep(1000);
		job = _queue.pop();

		assertTrue(job.getAttributes().getJID().startsWith(TEST_JID));
		assertTrue(job.getAttributes().getData().containsKey("url"));
		assertTrue(job.getAttributes().getData().get("url")
				.equals("http://www.homepage.com"));

		removeJobs(TEST_JID);
	}

	@Test
	public void testQueuePopSingleJob() throws LuaScriptException {
		String jid = addJob();

		BaseJob job = _queue.pop();

		assertEquals(jid, job.getAttributes().getJID());

		removeJobs(jid);
	}

	@Test
	public void testQueuePopMultipleJobs() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();

		List<BaseJob> jobs = _queue.pop(2);

		assertEquals(2, jobs.size());

		for (BaseJob baseJob : jobs) {
			assertTrue(baseJob.getAttributes().getJID().equals(jid1)
					|| baseJob.getAttributes().getJID().equals(jid2));
		}

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueuePeekTopJob() throws LuaScriptException,
			InterruptedException {
		String jid1 = addJob();
		Thread.sleep(1000);
		String jid2 = addJob();

		BaseJob job = _queue.peek();

		assertEquals(jid1, job.getAttributes().getJID());

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueuePeekMultipleJobs() throws LuaScriptException,
			InterruptedException {
		String jid1 = addJob();
		Thread.sleep(1000);
		String jid2 = addJob();

		List<BaseJob> jobs = _queue.peek(2);

		assertEquals(2, jobs.size());

		for (BaseJob baseJob : jobs) {
			assertTrue(baseJob.getAttributes().getJID().equals(jid1)
					|| baseJob.getAttributes().getJID().equals(jid2));
		}

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueuePeekDoesNotRemoveJobs() throws LuaScriptException,
			InterruptedException {
		String jid1 = addJob();
		Thread.sleep(1000);
		String jid2 = addJob();

		BaseJob job = _queue.peek();

		assertEquals(jid1, job.getAttributes().getJID());

		job = _queue.peek();

		assertEquals(jid1, job.getAttributes().getJID());

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueueGetStats() throws LuaScriptException {
		String jid = addNewTestJob();
		String jid2 = addJob();

		// Simulate Work
		popJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(TEST_QUEUE,
				JQlessClient.getCurrentSeconds());

		Map<String, Object> stats = _queue.stats();

		assertTrue(stats.containsKey("retries"));
		assertTrue(stats.containsKey("failed"));
		assertTrue(stats.containsKey("failures"));
		assertTrue(stats.containsKey("wait"));
		assertTrue(stats.containsKey("run"));

		removeJob(jid);
		removeJob(jid2);
	}

	@Test
	public void testQueueGetLength() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addDelayedJob(60);

		popJob();
		popJob();

		int length = _queue.length();

		assertEquals(2, length);

		removeJobs(jid1, jid2);
	}
}
