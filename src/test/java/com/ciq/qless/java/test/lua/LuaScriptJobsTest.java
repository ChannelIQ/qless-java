package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;

public class LuaScriptJobsTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "jobs.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Jobs";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues,
				"Got 1 expected 0 KEYS arguments");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingTypeArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Arg \"type\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidTypeArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("unknown-type",
				JQlessClient.getCurrentSeconds(), TEST_QUEUE);

		testArgsException(emptyValues, args, "Unknown type \"unknown-type\"");
	}

	@Test(expected = LuaScriptException.class)
	public void testCompleteInvalidOffsetArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("complete", "test-offset");

		testArgsException(emptyKeys, args,
				"Arg \"offset\" not a number: test-offset");
	}

	@Test(expected = LuaScriptException.class)
	public void testCompleteInvalidCountArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("complete", "0", "test-count");

		testArgsException(emptyKeys, args,
				"Arg \"count\" not a number: test-count");
	}

	@Test(expected = LuaScriptException.class)
	public void testOthersMissingNowArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("running");

		testArgsException(emptyKeys, args,
				"Arg \"now\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testOthersInvalidNowArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("running", "test-now");

		testArgsException(emptyKeys, args,
				"Arg \"now\" missing or not a number: test-now");
	}

	@Test(expected = LuaScriptException.class)
	public void testOthersMissingQueueArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("running",
				JQlessClient.getCurrentSeconds());

		testArgsException(emptyKeys, args, "Arg \"queue\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testOthersInvalidOffsetArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList("running",
				JQlessClient.getCurrentSeconds(), TEST_QUEUE, "test-offset");

		testArgsException(emptyKeys, args,
				"Arg \"offset\" not a number: test-offset");
	}

	@Test(expected = LuaScriptException.class)
	public void testOthersInvalidCountArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays
				.asList("running", JQlessClient.getCurrentSeconds(),
						TEST_QUEUE, "0", "test-count");

		testArgsException(emptyKeys, args,
				"Arg \"count\" not a number: test-count");
	}

	@Test
	public void testCompletedJobs() throws LuaScriptException {
		String jid = addNewTestJob();

		popJob();

		String jidComplete = completeJob(jid);
		assertEquals("complete", jidComplete);

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("complete");
		@SuppressWarnings("unchecked")
		List<String> jids = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		assertTrue(jids.contains(jid));

		removeJob(jid);
	}

	@Test
	public void testRunningJobs() throws LuaScriptException {
		String jid = addNewTestJob();

		popJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("running",
				JQlessClient.getCurrentSeconds(), TEST_QUEUE);
		@SuppressWarnings("unchecked")
		List<String> jids = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		assertTrue(jids.contains(jid));

		removeJob(jid);
	}

	@Test
	public void testStalledJobs() throws LuaScriptException,
			InterruptedException {
		// Set the heartbeat to 5 so we can simulate a stalled job
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays
				.asList("set", "heartbeat", String.valueOf(5));
		_luaScript.callScript("config.lua", keys, args);

		String jid = addNewTestJob();

		popJob();

		Thread.sleep(6000);

		args = Arrays.asList("stalled", JQlessClient.getCurrentSeconds(),
				TEST_QUEUE);
		@SuppressWarnings("unchecked")
		List<String> jids = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		assertTrue(jids.contains(jid));

		removeJob(jid);
	}

	@Test
	public void testScheduledJobs() throws LuaScriptException {
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(TEST_JID, "SimpleTestJob", "{}",
				JQlessClient.getCurrentSeconds(), "60");
		String jid = addJob(keys, args);

		popJob();

		keys = new ArrayList<String>();
		args = Arrays.asList("scheduled", JQlessClient.getCurrentSeconds(),
				TEST_QUEUE);
		@SuppressWarnings("unchecked")
		List<String> jids = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		assertTrue(jids.contains(jid));

		removeJob(jid);
	}

	@Test
	public void testDependsJob() throws LuaScriptException {
		String jidChild = addNewTestJob();
		String jidChild2 = addJob(UUID.randomUUID().toString());
		List<String> dependencies = Arrays.asList(jidChild, jidChild2);

		String jidParent = addDependentJob(dependencies);

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("depends",
				JQlessClient.getCurrentSeconds(), TEST_QUEUE);
		@SuppressWarnings("unchecked")
		List<String> jids = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		assertTrue(jids.contains(jidParent));

		removeJobs(jidParent, jidChild, jidChild2);
	}

	@Test
	public void testRecurringJob() throws LuaScriptException {
		String jid = addRecurringJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("recurring",
				JQlessClient.getCurrentSeconds(), TEST_QUEUE);
		@SuppressWarnings("unchecked")
		List<String> jids = (List<String>) _luaScript.callScript(
				this.scriptName(), keys, args);

		assertTrue(jids.contains(jid));

		removeRecurringJob(jid);
	}

}
