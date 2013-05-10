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

public class LuaScriptTrackTest extends LuaScriptBaseTest {

	@Override
	protected String scriptName() {
		return "track.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Track";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No keys expected. Got 1");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingJIDArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("track");

		testArgsException(emptyValues, args, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testUnknownActionArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("unknown-action", TEST_JID,
				JQlessClient.getCurrentSeconds());

		testArgsException(emptyValues, args,
				"Unknown action \"unknown-action\"");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("track", TEST_JID);

		testArgsException(emptyValues, badValues,
				"Arg \"now\" missing or not a number: nil");
	}

	@Test
	public void testTrackNewJob() throws LuaScriptException {
		String jid = addNewTestJob();

		performTrackingAction("track", jid);

		untrackAndRemove(jid);
	}

	@Test
	public void testUntrackJob() throws LuaScriptException {
		String jid = addNewTestJob();

		performTrackingAction("track", jid);

		performTrackingAction("untrack", jid);

		removeJob(jid);
	}

	@Test
	public void testShowAllTrackedJobs() throws LuaScriptException {
		String jid = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString(), TEST_QUEUE);

		// Track both jobs
		performTrackingAction("track", jid);
		performTrackingAction("track", jid2);

		// Get tracked status
		List<String> emptyKeys = new ArrayList<String>();
		String json = (String) _luaScript.callScript(this.scriptName(),
				emptyKeys, emptyKeys);

		// Fix potential parsing issues upfront
		json = JsonHelper.fixArrayField(json, "jobs");
		json = JsonHelper.fixArrayField(json, "expired");

		Map<String, Object> tracked = JsonHelper.parseMap(json);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> trackedJobs = (List<Map<String, Object>>) tracked
				.get("jobs");

		for (Map<String, Object> job : trackedJobs) {
			assertEquals("true", job.get("tracked").toString());

			if (job.get("jid").toString().equals(jid)) {
				assertEquals(jid, job.get("jid").toString());
			} else if (job.get("jid").toString().equals(jid2)) {
				assertEquals(jid2, job.get("jid").toString());
			}
		}

		untrackAndRemove(jid);
		untrackAndRemove(jid2);
	}

	private void untrackAndRemove(String jid) throws LuaScriptException {
		performTrackingAction("untrack", jid);
		removeJob(jid);
	}

	private void performTrackingAction(String action, String jid)
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(action, jid,
				JQlessClient.getCurrentSeconds());
		long result = (Long) _luaScript.callScript(this.scriptName(),
				emptyKeys, args);

		assertEquals(1, result);
	}
}
