package com.ciq.jqless.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.jqless.LuaScriptException;
import com.ciq.jqless.client.JQlessClient;

public class LuaScriptPopTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "pop.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Pop";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysEqualsZeroThrowsException() throws LuaScriptException {
		List<String> noValues = new ArrayList<String>();

		testArgsException(noValues, noValues, "Expected 1 KEYS argument");
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysGreaterThanOneThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key",
				"another-non-empty-key");

		testArgsException(badValues, badValues,
				"Got 2, expected 1 KEYS argument");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingWorkerArgsThrowsException()
			throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(keys, emptyValues, "Arg \"worker\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingCountArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER);

		testArgsException(keys, args,
				"Arg \"count\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidCountArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER, "test-count");

		testArgsException(keys, args,
				"Arg \"count\" missing or not a number: test-count");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER, "1");

		testArgsException(keys, args,
				"Arg \"now\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidNowArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER, "1", "test-now");

		testArgsException(keys, args,
				"Arg \"now\" missing or not a number: test-now");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPopASingleJob() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());

		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER, "1",
				JQlessClient.getCurrentSeconds());

		@SuppressWarnings("unchecked")
		List<String> json = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		Map<String, Object> jobs = parseMapFirstObject(json);

		assertTrue(json.size() == 1);
		if (jobs.get("jid").equals(jid1)) {
			assertEquals(jid1, jobs.get("jid"));
		} else {
			assertEquals(jid2, jobs.get("jid"));

			String jobJson = getJob(jid1);
			Map<String, Object> job1 = parseMap(jobJson);
			jobJson = getJob(jid2);
			Map<String, Object> job2 = parseMap(jobJson);

			assertEquals(((List<Map<String, Object>>) (job1.get("history")))
					.get(0).get("put"),
					((List<Map<String, Object>>) (job2.get("history"))).get(0)
							.get("put"));
		}

		removeJob(jid1);
		removeJob(jid2);
	}

	@Test
	public void testPopJobInsertedWithHigherPriority()
			throws LuaScriptException {
		String jid1 = addNewTestJob();

		// Make a second job with higher priority
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> jobArgs = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}", JQlessClient.getCurrentSeconds(), "0",
				"priority", "10");
		String jid2 = addJob(keys, jobArgs);

		List<String> args = Arrays.asList(TEST_WORKER, "1",
				JQlessClient.getCurrentSeconds());

		@SuppressWarnings("unchecked")
		List<String> json = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		Map<String, Object> jobs = parseMapFirstObject(json);

		assertTrue(json.size() == 1);
		assertEquals(jid2, jobs.get("jid"));
		assertEquals(10, jobs.get("priority"));

		removeJob(jid1);
		removeJob(jid2);
	}

	@Test
	public void testPopMultipleJobs() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());

		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER, "2",
				JQlessClient.getCurrentSeconds());

		@SuppressWarnings("unchecked")
		List<String> json = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		List<Map> jobs = parseList(json, List.class, Map.class);

		assertTrue(jobs.size() == 2);

		List<String> jids = new ArrayList<String>();
		for (Map<String, Object> job : jobs) {
			jids.add(job.get("jid").toString());
		}

		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));

		removeJob(jid1);
		removeJob(jid2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPopRemovesItemFromList() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());

		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER, "1",
				JQlessClient.getCurrentSeconds());

		// First pass
		List<String> json = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		Map<String, Object> jobs = parseMapFirstObject(json);

		assertTrue(json.size() == 1);
		if (jobs.get("jid").equals(jid1)) {
			assertEquals(jid1, jobs.get("jid"));
		} else {
			assertEquals(jid2, jobs.get("jid"));

			String jobJson = getJob(jid1);
			Map<String, Object> job1 = parseMap(jobJson);
			jobJson = getJob(jid2);
			Map<String, Object> job2 = parseMap(jobJson);

			assertEquals(((List<Map<String, Object>>) (job1.get("history")))
					.get(0).get("put"),
					((List<Map<String, Object>>) (job2.get("history"))).get(0)
							.get("put"));
		}

		// Second pass
		json = (List<String>) _luaScript.callScript(this.scriptName(), keys,
				args);

		jobs = parseMapFirstObject(json);

		assertTrue(json.size() == 1);
		if (jobs.get("jid").equals(jid2)) {
			assertEquals(jid2, jobs.get("jid"));
		} else {
			assertEquals(jid1, jobs.get("jid"));

			String jobJson = getJob(jid1);
			Map<String, Object> job1 = parseMap(jobJson);
			jobJson = getJob(jid2);
			Map<String, Object> job2 = parseMap(jobJson);

			assertEquals(((List<Map<String, Object>>) (job1.get("history")))
					.get(0).get("put"),
					((List<Map<String, Object>>) (job2.get("history"))).get(0)
							.get("put"));
		}

		removeJob(jid1);
		removeJob(jid2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPopCausesQueueToHaveLessItems() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());

		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_WORKER, "1",
				JQlessClient.getCurrentSeconds());

		// Test queue count
		testQueueCount("2", "0");

		// First pass
		List<String> json = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		Map<String, Object> jobs = parseMapFirstObject(json);

		assertTrue(json.size() == 1);
		if (jobs.get("jid").equals(jid1)) {
			assertEquals(jid1, jobs.get("jid"));
		} else {
			assertEquals(jid2, jobs.get("jid"));

			String jobJson = getJob(jid1);
			Map<String, Object> job1 = parseMap(jobJson);
			jobJson = getJob(jid2);
			Map<String, Object> job2 = parseMap(jobJson);

			assertEquals(((List<Map<String, Object>>) (job1.get("history")))
					.get(0).get("put"),
					((List<Map<String, Object>>) (job2.get("history"))).get(0)
							.get("put"));
		}

		// Test queue count
		testQueueCount("1", "1");

		removeJob(jid1);
		removeJob(jid2);
	}

	private void testQueueCount(String waitCount, String runCount)
			throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> queueArgs = Arrays.asList(
				JQlessClient.getCurrentSeconds(), TEST_QUEUE);

		String json = (String) _luaScript.callScript("queues.lua", noKeys,
				queueArgs);
		Map<String, Object> queueDetails = parseMap(json);

		assertEquals(TEST_QUEUE, queueDetails.get("name").toString());
		assertEquals(waitCount, queueDetails.get("waiting").toString());
		assertEquals(runCount, queueDetails.get("running").toString());
	}
}
