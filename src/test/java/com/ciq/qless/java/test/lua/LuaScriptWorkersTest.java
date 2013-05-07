package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ciq.qless.java.LuaScriptException;
import com.ciq.qless.java.client.JQlessClient;

public class LuaScriptWorkersTest extends LuaScriptTest {
	private String jid1;
	private String jid2;
	private String jid3;
	private String anotherWorkerJID;
	private final ArrayList<String> testWorkerJobs = new ArrayList<String>();

	@Override
	protected String scriptName() {
		return "workers.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Workers";
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
		final Map<String, Object> job = parseMapFirstObject(jsonList);
		anotherWorkerJID = job.get("jid").toString();
		testWorkerJobs.remove(anotherWorkerJID);
	}

	@After
	public void cleanupAndTeardownJobAndWorkers() throws LuaScriptException {
		removeJob(jid1);
		removeJob(jid2);
		removeJob(jid3);
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No key arguments expected");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues,
				"Arg \"now\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("test_now");

		testArgsException(emptyValues, badValues,
				"Arg \"now\" missing or not a number: test_now");
	}

	@Test
	public void testGetAllJobsForAWorker() throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds(),
				TEST_WORKER);

		String json = (String) _luaScript.callScript(this.scriptName(), noKeys,
				args);
		json = fixArrayField(json, "jobs");
		json = fixArrayField(json, "stalled");

		Map<String, Object> workerJobs = parseMap(json);

		@SuppressWarnings("unchecked")
		List<String> jobs = (List<String>) workerJobs.get("jobs");

		assertFalse(jobs.contains(anotherWorkerJID));
		assertTrue(testWorkerJobs.contains(jobs.get(0).toString()));
		assertTrue(testWorkerJobs.contains(jobs.get(1).toString()));
	}

	@Test
	public void testGetAllJobsCurrentOnWorkers() throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds());

		String json = (String) _luaScript.callScript(this.scriptName(), noKeys,
				args);
		json = fixArrayField(json, "jobs");
		json = fixArrayField(json, "stalled");
		List<Map<String, Object>> workers = parseList(json);

		for (Map<String, Object> worker : workers) {
			if (worker.get("name").equals(TEST_WORKER)) {
				assertEquals("2", worker.get("jobs").toString());
			} else {
				assertEquals("1", worker.get("jobs").toString());
			}
			assertEquals("0", worker.get("stalled").toString());
		}
	}

}
