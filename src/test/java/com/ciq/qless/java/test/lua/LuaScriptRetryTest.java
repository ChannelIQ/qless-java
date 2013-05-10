package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;

public class LuaScriptRetryTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "retry.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Retry";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "Got 1, expected 0");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingJIDArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingQueueArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID);

		testArgsException(emptyValues, badValues, "Arg \"queue\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingWorkerArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_QUEUE);

		testArgsException(emptyValues, badValues, "Arg \"worker\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_QUEUE,
				TEST_WORKER);

		testArgsException(emptyValues, badValues, "Arg \"now\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_QUEUE,
				TEST_WORKER, "test-now");

		testArgsException(emptyValues, badValues, "Arg \"now\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidDelayArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_QUEUE,
				TEST_WORKER, JQlessClient.getCurrentSeconds(), "test-delay");

		testArgsException(emptyValues, badValues,
				"Arg \"delay\" not a number: test-delay");
	}

	@Test
	public void testRetryJob() throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(TEST_JID, TEST_QUEUE, TEST_WORKER,
				JQlessClient.getCurrentSeconds(), "0");

		long result = (Long) _luaScript.callScript(this.scriptName(), noKeys,
				args);

		assertEquals(4, result);

		removeJob(jid);
	}

	@Test
	public void testRetryJobWithIncorrectWorker() throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(TEST_JID, TEST_QUEUE,
				"incorrect-worker", JQlessClient.getCurrentSeconds(), "0");

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertEquals("", result);

		removeJob(jid);
	}

	@Test
	public void testRetryJobNotInRunningState() throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(TEST_JID, TEST_QUEUE, TEST_WORKER,
				JQlessClient.getCurrentSeconds(), "0");

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertEquals("", result);

		removeJob(jid);
	}

	@Test
	public void testRetryWhereRemainingEqualsZero() {
		// Show that it is a failure

		// assert failures shows a new failure of "failed-retries-" + queue
		// assert return value==0

		fail("No tests yet");
	}
}
