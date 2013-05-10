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

public class LuaScriptHeartbeatTest extends LuaScriptBaseTest {

	@Override
	protected String scriptName() {
		return "heartbeat.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Heartbeat";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingJIDArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingWorkerArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID);

		testArgsException(emptyValues, badValues, "Arg \"worker\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_WORKER);

		testArgsException(emptyValues, badValues, "Arg \"now\" missing");
	}

	@Test
	public void testHeartbeatInvalidJob() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				TEST_WORKER, JQlessClient.getCurrentSeconds());

		String result = (String) _luaScript.callScript(this.scriptName(), keys,
				args);
		assertEquals("", result);

		removeJob(jid);
	}

	@Test
	public void testHeartbeatJob() throws LuaScriptException {
		String jid = addNewTestJob();
		String now = JQlessClient.getCurrentSeconds();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, now);

		popJob();

		long newExpires = (Long) _luaScript.callScript(this.scriptName(), keys,
				args);
		assertEquals(Long.valueOf(now) + 60, newExpires);

		removeJob(jid);
	}

	@Test
	public void testHeartbeatJobWithUpdatedData() throws LuaScriptException {
		String jid = addNewTestJob();
		String now = JQlessClient.getCurrentSeconds();

		String json = getJob(jid);

		popJob();

		List<String> data = Arrays.asList("test-data");
		String jsonData = createJSON(data);

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, now, jsonData);
		_luaScript.callScript(this.scriptName(), keys, args);

		json = getJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);

		@SuppressWarnings("unchecked")
		List<String> jobData = (List<String>) job.get("data");
		assertEquals("test-data", jobData.get(0).toString());

		removeJob(jid);
	}
}
