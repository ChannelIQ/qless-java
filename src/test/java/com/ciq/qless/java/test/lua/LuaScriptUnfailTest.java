package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.utils.JsonHelper;

public class LuaScriptUnfailTest extends LuaScriptBaseTest {

	@Override
	protected String scriptName() {
		return "unfail.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Unfail";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "Expected 0 KEYS arguments");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Arg \"now\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingGroupArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays
				.asList(JQlessClient.getCurrentSeconds());

		testArgsException(emptyValues, badValues, "Arg \"group\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingQueueArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(
				JQlessClient.getCurrentSeconds(), "test-group");

		testArgsException(emptyValues, badValues, "Arg \"queue\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidCountArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(
				JQlessClient.getCurrentSeconds(), "test-group", TEST_QUEUE,
				"test-count");

		testArgsException(emptyValues, badValues,
				"Arg \"count\" not a number: test-count");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUnfailSingleJobForGroupIntoNewQueue()
			throws LuaScriptException {
		String jid = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());

		String jidFailed = failJob("test-group");

		// Get job and validate
		String json = getJob(jidFailed);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals("failed", job.get("state"));
		assertEquals(TEST_QUEUE, job.get("queue"));
		Map<String, Object> failure = (Map<String, Object>) job.get("failure");
		assertEquals("test-group", failure.get("group").toString());

		// Now unfail into a new queue
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds(),
				"test-group", "new-queue", "1");
		long jidUnfailed = (Long) _luaScript.callScript(this.scriptName(),
				noKeys, args);
		assertEquals(1, jidUnfailed);

		// Now get the job again and verify status and queue
		json = getJob(jidFailed);
		job = JsonHelper.parseMap(json);
		System.out.println(json);
		assertEquals("new-queue", job.get("queue"));
		assertEquals("waiting", job.get("state"));

		removeJob(jid);
		removeJob(jid2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUnfailMultipleJobsForGroup() throws LuaScriptException {
		String jid = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());
		String jid3 = addJob(UUID.randomUUID().toString());

		String jidFailed = failJob("test-group");
		String jidFailed2 = failJob("test-group");

		// Get job jid and validate
		String json = getJob(jidFailed);
		System.out.println(json);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals("failed", job.get("state"));
		assertEquals(TEST_QUEUE, job.get("queue"));
		Map<String, Object> failure = (Map<String, Object>) job.get("failure");
		assertEquals("test-group", failure.get("group").toString());

		// Get job jid2 and validate
		json = getJob(jidFailed2);
		System.out.println(json);
		job = JsonHelper.parseMap(json);
		assertEquals("failed", job.get("state"));
		assertEquals(TEST_QUEUE, job.get("queue"));
		failure = (Map<String, Object>) job.get("failure");
		assertEquals("test-group", failure.get("group").toString());

		// Now unfail into a new queue
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds(),
				"test-group", "new-queue", "2");
		long jidUnfailed = (Long) _luaScript.callScript(this.scriptName(),
				noKeys, args);
		assertEquals(2, jidUnfailed);

		// Now get the job (jid) again and verify status and queue
		json = getJob(jidFailed);
		job = JsonHelper.parseMap(json);
		System.out.println(json);
		assertEquals("new-queue", job.get("queue"));
		assertEquals("waiting", job.get("state"));

		// Now get the job (jid2) again and verify status and queue
		json = getJob(jidFailed2);
		job = JsonHelper.parseMap(json);
		System.out.println(json);
		assertEquals("new-queue", job.get("queue"));
		assertEquals("waiting", job.get("state"));

		removeJob(jid);
		removeJob(jid2);
		removeJob(jid3);
	}

	private String failJob(String group) throws LuaScriptException {
		// Pop the job to simulate that it is running (Only running jobs can be
		// failed)
		List<String> poppedJob = popJob();

		Map<String, Object> job = JsonHelper.parseMapFirstObject(poppedJob);

		// Fail the job
		List<String> emptyValues = new ArrayList<String>();
		List<String> failArgs = Arrays.asList(job.get("jid").toString(),
				TEST_WORKER, group, "test-message",
				JQlessClient.getCurrentSeconds());

		String failedJID = (String) _luaScript.callScript("fail.lua",
				emptyValues, failArgs);

		assertEquals(job.get("jid"), failedJID);

		return job.get("jid").toString();
	}
}
