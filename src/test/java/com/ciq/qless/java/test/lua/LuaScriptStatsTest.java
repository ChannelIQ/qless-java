package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.LuaScriptException;
import com.ciq.qless.java.client.JQlessClient;

public class LuaScriptStatsTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "stats.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Stats";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingQueueArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Arg \"queue\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingTimeArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID);

		testArgsException(emptyValues, badValues,
				"Arg \"time\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidTimeArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, "test-time");

		testArgsException(emptyValues, badValues,
				"Arg \"time\" missing or not a number: test-time");
	}

	@Test
	public void testStatsReturnSuccessful() throws LuaScriptException {
		String jid = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());

		// Simulate Work
		popJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(TEST_QUEUE,
				JQlessClient.getCurrentSeconds());

		String json = (String) _luaScript.callScript(this.scriptName(), keys,
				args);

		System.out.println(json);
		Map<String, Object> stats = parseMap(json);

		assertTrue(stats.containsKey("retries"));
		assertTrue(stats.containsKey("failed"));
		assertTrue(stats.containsKey("failures"));
		assertTrue(stats.containsKey("wait"));
		assertTrue(stats.containsKey("run"));

		removeJob(jid);
		removeJob(jid2);
	}
}
