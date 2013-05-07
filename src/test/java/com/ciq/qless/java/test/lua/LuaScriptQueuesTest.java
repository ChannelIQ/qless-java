package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;

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

public class LuaScriptQueuesTest extends LuaScriptTest {
	private String jid1;
	private String jid2;
	private String jid3;
	private final String ANOTHER_QUEUE = "another-queue";

	@Override
	protected String scriptName() {
		return "queues.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Queues";
	}

	@Before
	public void setupJobAndWorkers() throws LuaScriptException {
		jid1 = addNewTestJob();
		jid2 = addJob(UUID.randomUUID().toString(), TEST_QUEUE);
		jid3 = addJob(UUID.randomUUID().toString(), ANOTHER_QUEUE);

		// Pop job to simulate worker
		popJob();
	}

	@After
	public void cleanupAndTeardown() throws LuaScriptException {
		removeJob(jid1);
		removeJob(jid2);
		removeJob(jid3);
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues,
				"Got 1 expected 0 KEYS arguments");
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
		List<String> args = Arrays.asList("fake_now");

		testArgsException(emptyValues, args,
				"Arg \"now\" missing or not a number: fake_now");
	}

	@Test
	public void testViewDetailsForSingleQueue() throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds(),
				TEST_QUEUE);

		String json = (String) _luaScript.callScript(this.scriptName(), noKeys,
				args);

		Map<String, Object> queueDetails = parseMap(json);

		assertEquals(TEST_QUEUE, queueDetails.get("name").toString());
		assertEquals("1", queueDetails.get("running").toString());
		assertEquals("1", queueDetails.get("waiting").toString());
	}

	@Test
	public void testViewDetailsForAllQueues() throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(JQlessClient.getCurrentSeconds());

		String json = (String) _luaScript.callScript(this.scriptName(), noKeys,
				args);

		List<Map<String, Object>> queueDetails = parseList(json);

		for (Map<String, Object> queue : queueDetails) {
			if (queue.get("name").equals(TEST_QUEUE)) {
				assertEquals("1", queue.get("running").toString());
				assertEquals("1", queue.get("waiting").toString());
			} else {
				assertEquals("0", queue.get("running").toString());
				assertEquals("1", queue.get("waiting").toString());
			}
			assertEquals("0", queue.get("scheduled").toString());
		}
	}

}
