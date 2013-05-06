package com.ciq.jqless.test.lua;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.jqless.LuaScriptException;
import com.ciq.jqless.client.JQlessClient;

public class LuaScriptCompleteTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "complete.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Complete";
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

		testArgsException(emptyValues, emptyValues, "Arg \"jid\" missing.");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingWorkerArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID);

		testArgsException(emptyValues, badValues, "Arg \"worker\" missing.");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingQueueArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, "test_worker");

		testArgsException(emptyValues, badValues, "Arg \"queue\" missing.");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, "test_worker",
				TEST_QUEUE);

		testArgsException(emptyValues, badValues,
				"Arg \"now\" not a number or missing: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, "test_worker",
				TEST_QUEUE, "test_now");

		testArgsException(emptyValues, badValues,
				"Arg \"now\" not a number or missing: test_now");
	}

	@Test(expected = LuaScriptException.class)
	public void testDelayAndDependsArgsUsedTogetherThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		String dependsJID = UUID.randomUUID().toString();
		List<String> values = Arrays.asList(dependsJID);
		String dependsJson = createJSON(values);

		List<String> badValues = Arrays.asList(TEST_JID, "test_worker",
				TEST_QUEUE, JQlessClient.getCurrentSeconds(), "{}", "next",
				"next_queue", "delay", "1", "depends", dependsJson);

		testArgsException(emptyValues, badValues,
				"\"delay\" and \"depends\" are not allowed to be used together");
	}

	@Test(expected = LuaScriptException.class)
	public void testDelayRequiresNextOrThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, "test_worker",
				TEST_QUEUE, JQlessClient.getCurrentSeconds(), "{}", "delay",
				"1");

		testArgsException(emptyValues, badValues,
				"\"delay\" cannot be used without a \"next\".");
	}

	@Test(expected = LuaScriptException.class)
	public void testDependsRequiresNextOrThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		String dependsJID = UUID.randomUUID().toString();
		List<String> values = Arrays.asList(dependsJID);
		String dependsJson = createJSON(values);

		List<String> badValues = Arrays.asList(TEST_JID, "test_worker",
				TEST_QUEUE, JQlessClient.getCurrentSeconds(), "{}", "depends",
				dependsJson);

		testArgsException(emptyValues, badValues,
				"\"depends\" cannot be used without a \"next\".");
	}

	@Test
	public void testJobComplete() throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}");

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertEquals("complete", result);

		removeJob(jid);
	}

	@Test
	public void testJobCompleteWithIncorrectWorker() throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, "", TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}");

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertEquals("", result);

		removeJob(jid);
	}

	@Test
	public void testJobCannotCompleteIfStateIsNotRunning()
			throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, "", TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}");

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertEquals("", result);

		removeJob(jid);
	}

	@Test
	public void testJobCompleteMovedToNewQueueWaiting()
			throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it with a next queue, zero delay
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}", "next", "next-queue");

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		// assert Waiting
		assertEquals("waiting", result);

		String json = getJob(jid);
		Map<String, Object> job = parseMap(json);

		assertEquals("waiting", job.get("state").toString());
		assertEquals("next-queue", job.get("queue").toString());

		removeJob(jid);
	}

	@Test
	public void testJobCompleteMovedToNewQueueScheduled()
			throws LuaScriptException {
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it with a next queue, some delay
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}", "next", "next-queue",
				"delay", "60");

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		// assert Scheduled
		assertEquals("scheduled", result);

		String json = getJob(jid);
		Map<String, Object> job = parseMap(json);

		assertEquals("waiting", job.get("state").toString());
		assertEquals("next-queue", job.get("queue").toString());

		removeJob(jid);
	}

	@Test
	public void testJobCompleteMovedToNewQueueWithDependentsCompleted()
			throws LuaScriptException {
		// Create dependents/perform work/complete jobs
		String jidChild1 = addJob(UUID.randomUUID().toString());
		String jidChild2 = addJob(UUID.randomUUID().toString());
		String dependentsJson = createJSON(Arrays.asList(jidChild1, jidChild2));

		popJob();
		popJob();

		completeJob(jidChild1);
		completeJob(jidChild2);

		// Create new job
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it with a next queue, no delay, with
		// dependents all completed
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}", "next", "next-queue",
				"depends", dependentsJson);

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		// assert Waiting
		assertEquals("waiting", result);

		String json = getJob(jid);
		Map<String, Object> job = parseMap(json);

		assertEquals("waiting", job.get("state").toString());
		assertEquals("next-queue", job.get("queue").toString());

		removeJobs(Arrays.asList(jid, jidChild1, jidChild2));
	}

	@Test
	public void testJobCompleteMovedToNewQueueWithDependentsNotCompleted()
			throws LuaScriptException {
		// Create dependents/perform work/complete jobs
		String jidChild1 = addJob(UUID.randomUUID().toString());
		String jidChild2 = addJob(UUID.randomUUID().toString());
		String dependentsJson = createJSON(Arrays.asList(jidChild1, jidChild2));

		popJob();
		popJob();

		// Create new job
		String jid = addNewTestJob();
		assertEquals(TEST_JID, jid);

		// Set job to running then Complete it with a next queue, no delay, with
		// dependents all completed
		popJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}", "next", "next-queue",
				"depends", dependentsJson);

		String result = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		// assert Waiting
		assertEquals("depends", result);

		String json = getJob(jid);
		Map<String, Object> job = parseMap(json);

		assertEquals("depends", job.get("state").toString());
		assertEquals("next-queue", job.get("queue").toString());

		// Now complete the dependents
		completeJob(jidChild1);
		completeJob(jidChild2);

		json = getJob(jid);
		job = parseMap(json);
		assertEquals("waiting", job.get("state").toString());
		assertEquals("next-queue", job.get("queue").toString());

		removeJobs(Arrays.asList(jid, jidChild1, jidChild2));
	}
}
