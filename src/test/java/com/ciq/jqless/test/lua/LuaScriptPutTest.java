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

public class LuaScriptPutTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "put.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Put";
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
	public void testMissingJIDArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(keys, emptyValues, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingKlassArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(UUID.randomUUID().toString());

		testArgsException(keys, args, "Arg \"klass\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}");

		testArgsException(keys, args,
				"Arg \"now\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidNowArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}", "test-now");

		testArgsException(keys, args,
				"Arg \"now\" missing or not a number: test-now");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingDelayArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}", JQlessClient.getCurrentSeconds());

		testArgsException(keys, args, "Arg \"delay\" not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidDelayArgsThrowsException() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}", JQlessClient.getCurrentSeconds(),
				"test-delay");

		testArgsException(keys, args, "Arg \"delay\" not a number: test-delay");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidRetriesArgsThrowsException()
			throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}", JQlessClient.getCurrentSeconds(), "0",
				"retries", "test-retries");

		testArgsException(keys, args,
				"Arg \"retries\" not a number: test-retries");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidPriorityArgsThrowsException()
			throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}", JQlessClient.getCurrentSeconds(), "0",
				"priority", "test-priority");

		testArgsException(keys, args,
				"Arg \"priority\" not a numbertest-priority");
	}

	@Test(expected = LuaScriptException.class)
	public void testDelayAndDependsTogetherThrowsException()
			throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		String dependsJson = createJSON(Arrays.asList(UUID.randomUUID()
				.toString()));
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				"SimpleTestClass", "{}", JQlessClient.getCurrentSeconds(), "1",
				"depends", dependsJson);

		testArgsException(keys, args,
				"\"delay\" and \"depends\" are not allowed to be used together");
	}

	@Test
	public void testPutAddNewJob() throws LuaScriptException {
		String jid = addNewTestJob();

		assertEquals(jid, TEST_JID);

		String scriptResult = getJob(TEST_JID);

		Map<String, Object> job = parseMap(scriptResult);

		assertTrue(job.containsKey("jid"));
		assertTrue(job.get("jid").equals(jid));
		assertEquals("SimpleTestJob", job.get("klass").toString());
		assertEquals("waiting", job.get("state").toString());

		removeJob(jid);
	}

	@Test
	public void testPutNewJobWithDelay() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_JID, "SimpleTestJob", "{}",
				JQlessClient.getCurrentSeconds(), "60");

		String jid = (String) _luaScript.callScript(this.scriptName(), keys,
				args);

		assertEquals(jid, TEST_JID);

		String scriptResult = getJob(TEST_JID);
		Map<String, Object> result = parseMap(scriptResult);

		assertTrue(result.containsKey("jid"));
		assertTrue(result.get("jid").equals(jid));
		assertEquals("scheduled", result.get("state"));

		removeJob(jid);
	}

}
