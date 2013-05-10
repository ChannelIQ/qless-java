package com.ciq.qless.java.test.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.client.ClientWorkers;
import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.test.BaseTest;
import com.ciq.qless.java.utils.JsonHelper;

public class ClientWorkersTest extends BaseTest {

	private static ClientWorkers _clientWorkers;
	private String jid1;
	private String jid2;
	private String jid3;
	private String anotherWorkerJID;
	private final ArrayList<String> testWorkerJobs = new ArrayList<String>();
	private final String ANOTHER_WORKER = "another-worker";

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		_clientWorkers = new ClientWorkers(new JQlessClient(jedis));
	}

	@Before
	public void setupJobAndWorkers() throws LuaScriptException {
		jid1 = addNewTestJob();
		jid2 = addJob(UUID.randomUUID().toString(), TEST_QUEUE);
		jid3 = addJob(UUID.randomUUID().toString(), TEST_QUEUE);
		testWorkerJobs.add(jid1);
		testWorkerJobs.add(jid2);
		testWorkerJobs.add(jid3);

		// Setup workers
		popJob();
		popJob();
		List<String> jsonList = popJob("another-worker");
		final Map<String, Object> job = JsonHelper
				.parseMapFirstObject(jsonList);
		anotherWorkerJID = job.get("jid").toString();
		testWorkerJobs.remove(anotherWorkerJID);
	}

	@After
	public void cleanupAndTeardownJobAndWorkers() throws LuaScriptException {
		removeJob(jid1);
		removeJob(jid2);
		removeJob(jid3);
	}

	@Test
	public void testClientWorkersCount() {
		List<Map<String, Object>> workers = _clientWorkers.counts();

		for (Map<String, Object> worker : workers) {
			if (worker.get("name").equals(TEST_WORKER)) {
				assertEquals("2", worker.get("jobs").toString());
			} else {
				assertEquals("1", worker.get("jobs").toString());
			}
			assertEquals("0", worker.get("stalled").toString());
		}
	}

	@Test
	public void testClientWorkersGetWorkerByName() {
		Map<String, Object> workerJobs = _clientWorkers
				.getWorkersByName(TEST_WORKER);

		@SuppressWarnings("unchecked")
		List<String> jobs = (List<String>) workerJobs.get("jobs");

		assertFalse(jobs.contains(anotherWorkerJID));
		assertTrue(testWorkerJobs.contains(jobs.get(0).toString()));
		assertTrue(testWorkerJobs.contains(jobs.get(1).toString()));

		workerJobs = _clientWorkers.getWorkersByName(ANOTHER_WORKER);

		jobs = (List<String>) workerJobs.get("jobs");

		assertTrue(jobs.contains(anotherWorkerJID));
		for (String job : jobs) {
			assertFalse(testWorkerJobs.contains(job));
		}
	}
}
