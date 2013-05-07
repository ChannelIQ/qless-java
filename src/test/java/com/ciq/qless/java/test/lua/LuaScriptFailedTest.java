package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.LuaScriptException;
import com.ciq.qless.java.client.JQlessClient;

public class LuaScriptFailedTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "failed.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Failed";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidStartArgsThrowsException() throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("test-group", "test-start");

		testArgsException(noKeys, args,
				"Arg \"start\" is not a number: test-start");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidLimitArgsThrowsException() throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("test-group", "0", "test-limit");

		testArgsException(noKeys, args,
				"Arg \"limit\" is not a number: test-limit");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testFindFailuresForSingleGroup() throws LuaScriptException {
		String jid = addNewTestJob();

		String failedJid = failAJob("test-group");

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("test-group");
		String json = (String) _luaScript.callScript(this.scriptName(), noKeys,
				args);

		Map<String, Object> failures = parseMap(json);
		assertEquals(1, failures.get("total"));

		List<Map<String, Object>> jobs = (List<Map<String, Object>>) failures
				.get("jobs");
		assertEquals(jid, jobs.get(0).get("jid"));

		removeJob(jid);
		removeJob(failedJid);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testFindFailuresForAllGroups() throws LuaScriptException {
		String jid = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());
		String failedJid = failAJob("test-group");
		String failedJid2 = failAJob("test-group");

		String anotherJid = addJob(UUID.randomUUID().toString());
		String anotherFailedJid = failAJob("another-test-group");

		List<String> noValues = new ArrayList<String>();
		String json = (String) _luaScript.callScript(this.scriptName(),
				noValues, noValues);

		Map<String, Object> failures = parseMap(json);
		assertEquals(2, failures.get("test-group"));
		assertEquals(1, failures.get("another-test-group"));

		removeJob(jid);
		removeJob(jid2);
		removeJob(failedJid);
		removeJob(failedJid2);
		removeJob(anotherJid);
		removeJob(anotherFailedJid);
	}

	private String failAJob(String group) throws LuaScriptException {
		// Pop the job to simulate that it is running (Only running jobs can be
		// failed)
		List<String> json = popJob();
		Map<String, Object> job = parseMapFirstObject(json);
		String jid = job.get("jid").toString();

		// Fail the job
		List<String> emptyValues = new ArrayList<String>();
		List<String> failArgs = Arrays.asList(jid, TEST_WORKER, group,
				"test-message", JQlessClient.getCurrentSeconds());

		String failedJID = (String) _luaScript.callScript("fail.lua",
				emptyValues, failArgs);
		assertEquals(jid, failedJID);

		return jid;
	}
}
