package com.ciq.jqless.test.lua;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.ciq.jqless.LuaScriptException;
import com.ciq.jqless.client.JQlessClient;

public class LuaScriptFailTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "fail.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Fail";
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
	public void testMissingGroupArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_WORKER);

		testArgsException(emptyValues, badValues, "Arg \"group\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingMessageArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_WORKER,
				"test-group");

		testArgsException(emptyValues, badValues, "Arg \"message\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, TEST_WORKER,
				"test-group", "test-message");

		testArgsException(emptyValues, badValues,
				"Arg \"now\" missing or malformed: nil");
	}

	@Test
	public void testFailJobSuccessful() throws LuaScriptException {
		String jid = addNewTestJob();

		// Pop the job to simulate that it is running (Only running jobs can be
		// failed)
		popJob();

		// Fail the job
		List<String> emptyValues = new ArrayList<String>();
		List<String> failArgs = Arrays.asList(jid, TEST_WORKER, "test-group",
				"test-message", JQlessClient.getCurrentSeconds());

		String failedJID = (String) _luaScript.callScript(this.scriptName(),
				emptyValues, failArgs);

		assertEquals(jid, failedJID);

		removeJob(jid);
	}

	@Test
	public void testFailUnknownJobFails() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				TEST_WORKER, "test-group", "test-message",
				JQlessClient.getCurrentSeconds());

		String emptyString = (String) _luaScript.callScript(this.scriptName(),
				emptyValues, args);

		assertEquals("", emptyString);

		removeJob(jid);
	}
}
