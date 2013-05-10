package com.ciq.qless.java.test.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.client.Config;
import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.client.QlessClientException;
import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.jobs.JobOptions;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.test.BaseTest;
import com.ciq.qless.java.test.jobs.SimpleTestJob;

public class JQlessClientTest extends BaseTest {

	private static JQlessClient _client;

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		_client = new JQlessClient(jedis);
	}

	@Test
	public void testExecuteClientJobs() throws QlessClientException,
			LuaScriptException {
		String jid = addJob();

		BaseJob job = _client.Jobs().getJob(jid);

		assertEquals(jid, job.getAttributes().getJID());

		_client.cancel(jid);

		try {
			job = _client.Jobs().getJob(jid);
		} catch (QlessClientException qce) {
			assertTrue(true);
		}
	}

	@Test
	public void testExecuteClientWorkers() throws LuaScriptException {
		String jid1 = addJob();

		popJob();

		List<Map<String, Object>> workers = _client.Workers().counts();

		for (Map<String, Object> worker : workers) {
			if (worker.get("name").equals(TEST_WORKER)) {
				assertEquals("1", worker.get("jobs").toString());
			} else {
				assertEquals("0", worker.get("jobs").toString());
			}
			assertEquals("0", worker.get("stalled").toString());
		}

		_client.cancel(jid1);
	}

	@Test
	public void testCreatingANewQueue() throws QlessClientException {
		Queue testQueue = _client.Queues().getNamedQueue("test_queue");

		Map<String, Object> data = new HashMap<String, Object>();
		JobOptions options = new JobOptions.OptionsBuilder(UUID.randomUUID()
				.toString()).build();

		testQueue.put(SimpleTestJob.class.getName(), data, options);

		BaseJob job = testQueue.pop();
		String jid = job.getAttributes().getJID();
		assertEquals("test_queue", job.getAttributes().getQueueName());

		job.performWork();
		job.complete();

		job = _client.Jobs().getJob(jid);

		assertEquals("complete", job.getAttributes().getState());
	}

	@Test
	public void testGetQueueLength() throws QlessClientException,
			LuaScriptException {
		int zero = _client.getQueueLength("test_queue");
		assertEquals(0, zero);

		String jid = addJob();

		BaseJob job = _client.Jobs().getJob(jid);
		assertEquals(jid, job.getAttributes().getJID());
		assertEquals("waiting", job.getAttributes().getState());

		int one = _client.getQueueLength(TEST_QUEUE);
		assertEquals(1, one);

		job = _client.Queues().getNamedQueue(TEST_QUEUE).pop();
		assertEquals(jid, job.getAttributes().getJID());
		assertEquals("running", job.getAttributes().getState());

		_client.cancel(jid);
	}

	@Test
	public void testGetAndSetConfig() {
		_client.setConfig(Config.HEARTBEAT, "120");

		String heartbeat = _client.getConfig(Config.HEARTBEAT);
		assertEquals("120", heartbeat);

		_client.Config().clear(Config.HEARTBEAT);
	}

	@Test
	public void testTrackJob() throws QlessClientException, LuaScriptException {
		String jid = addJob();

		_client.track(jid);

		BaseJob job = _client.Jobs().getJob(jid);

		assertEquals(jid, job.getAttributes().getJID());
		assertEquals(true, job.getAttributes().isTracked());

		// TODO: Add event notification verification

		_client.cancel(jid);
	}

	@Test
	public void testUntrackJob() throws QlessClientException,
			LuaScriptException {
		String jid = addJob();

		_client.track(jid);

		BaseJob job = _client.Jobs().getJob(jid);

		assertEquals(jid, job.getAttributes().getJID());
		assertEquals(true, job.getAttributes().isTracked());

		// TODO: Add event notification verification

		_client.untrack(jid);
		job = _client.Jobs().getJob(jid);

		assertEquals(jid, job.getAttributes().getJID());
		assertEquals(false, job.getAttributes().isTracked());

		_client.cancel(jid);
	}

	@Test
	public void testGetPopularTags() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());
		String jid3 = addJob(UUID.randomUUID().toString());
		String jid4 = addJob(UUID.randomUUID().toString());

		addTags(jid1, "common-tag", "test-tag", "test-tag-1");
		addTags(jid2, "common-tag", "test-tag", "test-tag-2");
		addTags(jid3, "common-tag", "test-tag-3");
		addTags(jid4, "common-tag", "test-tag-4");

		List<String> tags = _client.tags(0, 2);
		assertTrue(tags.get(0).equals("common-tag"));
		assertTrue(tags.get(1).equals("test-tag"));
		assertFalse(tags.contains("test-tag-1"));

		// Get the top results
		tags = _client.tags(2, 4);

		assertTrue(tags.contains("test-tag-1"));
		assertTrue(tags.contains("test-tag-2"));
		assertTrue(tags.contains("test-tag-3"));
		assertTrue(tags.contains("test-tag-4"));

		// Cleanup
		removeJobs(jid1, jid2, jid3, jid4);
	}

	@Test
	public void testDeregisterWorkers() throws LuaScriptException {
		String jid = addJob();

		popJob();

		List<Map<String, Object>> workers = _client.Workers().counts();

		for (Map<String, Object> worker : workers) {
			if (worker.get("name").equals(TEST_WORKER)) {
				assertEquals("1", worker.get("jobs").toString());
			}
			assertEquals("0", worker.get("stalled").toString());
		}

		_client.deRegisterWorkers(TEST_WORKER);

		workers = _client.Workers().counts();
		assertEquals(null, workers);

		_client.cancel(jid);
	}

	@Test(expected = QlessClientException.class)
	public void testCancelJobs() throws QlessClientException,
			LuaScriptException {
		String jid = addJob();

		_client.cancel(jid);

		BaseJob job = _client.Jobs().getJob(jid);
		fail("Exception expected since the job has been removed: " + jid);
	}
}
