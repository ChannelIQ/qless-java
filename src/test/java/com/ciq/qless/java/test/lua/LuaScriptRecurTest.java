package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.utils.JsonHelper;

public class LuaScriptRecurTest extends LuaScriptBaseTest {

	@Override
	protected String scriptName() {
		return "recur.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Recur";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues,
				"Got 1, expected 0 KEYS arguments");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingCommandArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Missing first argument");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidCommandArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("command");

		testArgsException(emptyValues, badValues,
				"First argument must be one of [on, off, get, update, tag, untag]. Got command");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnMissingQueueArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on");

		testArgsException(emptyValues, badValues, "Arg \"queue\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnMissingJIDArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE);

		testArgsException(emptyValues, badValues, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnMissingKlassArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString());

		testArgsException(emptyValues, badValues, "Arg \"klass\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}");

		testArgsException(emptyValues, badValues,
				"Arg \"now\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnInvalidNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", "test-now");

		testArgsException(emptyValues, badValues,
				"Arg \"now\" missing or not a number: test-now");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnMissingScheduleTypeArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds());

		testArgsException(emptyValues, badValues,
				"Arg \"schedule type\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnScheduleIntervalMissingIntervalArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "interval");

		testArgsException(emptyValues, badValues,
				"Arg \"interval\" must be a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnScheduleIntervalInvalidIntervalArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "interval", "test-interval");

		testArgsException(emptyValues, badValues,
				"Arg \"interval\" must be a number: test-interval");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnScheduleIntervalMissingOffsetArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "interval", "0");

		testArgsException(emptyValues, badValues,
				"Arg \"offset\" must be a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnScheduleIntervalInvalidOffsetArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "interval", "0", "test-offset");

		testArgsException(emptyValues, badValues,
				"Arg \"offset\" must be a number: test-offset");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnScheduleIntervalInvalidIntervalEqualsZeroArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "interval", "0", "0");

		testArgsException(emptyValues, badValues,
				"Arg \"interval\" must be greater than or equal to 0");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnInvalidPriorityArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "interval", "1", "0", "priority",
				"test-priority");

		testArgsException(emptyValues, badValues,
				"Arg \"priority\" must be a number. Got: test-priority");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnInvalidRetriesArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "interval", "1", "0", "retries",
				"test-retries");

		testArgsException(emptyValues, badValues,
				"Arg \"retries\" must be a number. Got: test-retries");
	}

	@Test(expected = LuaScriptException.class)
	public void testOnInvalidScheduleTypeArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("on", TEST_QUEUE, UUID
				.randomUUID().toString(), "SimpleTestClass", "{}", JQlessClient
				.getCurrentSeconds(), "test-schedule", "1", "0");

		testArgsException(emptyValues, badValues,
				"schedule type \"test-schedule\" unknown");
	}

	@Test(expected = LuaScriptException.class)
	public void testOffMissingJIDArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("off");

		testArgsException(emptyValues, badValues, "Arg \"jid\" missing");
	}

	@Test
	public void testOffUnknownQueueReturnsEmptyString()
			throws LuaScriptException {
		String jid = addNewTestJob();

		// Since this job is not recurring it simulates an unknown jid (which
		// means queue is unknown or missing)

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("off", jid);

		long result = (Long) _luaScript.callScript(this.scriptName(), keys,
				args);

		assertEquals(1, result);

		removeJob(jid);
	}

	@Test
	public void testGetUnknownJIDReturnsEmptyString() throws LuaScriptException {
		String jid = addNewTestJob();

		// Since this job is not recurring it simulates an unknown jid

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("get", jid);

		String result = (String) _luaScript.callScript(this.scriptName(), keys,
				args);

		assertEquals("", result);

		removeJob(jid);
	}

	@Test(expected = LuaScriptException.class)
	public void testUpdateMissingJIDArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("update");

		testArgsException(emptyValues, badValues, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testUpdateInvalidPriorityArgsThrowsException()
			throws LuaScriptException {
		String jid = addRecurringJob();

		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("update", jid, "priority",
				"test-priority");

		try {
			testArgsException(emptyValues, badValues,
					"Arg \"priority\" must be a number: test-priority");
		} catch (LuaScriptException ex) {
			throw ex;
		} finally {
			removeRecurringJob(jid);
		}
	}

	@Test(expected = LuaScriptException.class)
	public void testUpdateInvalidRetriesArgsThrowsException()
			throws LuaScriptException {
		String jid = addRecurringJob();

		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("update", jid, "retries",
				"test-retries");

		try {
			testArgsException(emptyValues, badValues,
					"Arg \"retries\" must be a number: test-retries");
		} catch (LuaScriptException ex) {
			throw ex;
		} finally {
			removeRecurringJob(jid);
		}
	}

	@Test(expected = LuaScriptException.class)
	public void testUpdateInvalidIntervalArgsThrowsException()
			throws LuaScriptException {
		String jid = addRecurringJob();

		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("update", jid, "interval",
				"test-interval");

		try {
			testArgsException(emptyValues, badValues,
					"Arg \"interval\" must be a number: test-interval");
		} catch (LuaScriptException ex) {
			throw ex;
		} finally {
			removeRecurringJob(jid);
		}
	}

	@Test(expected = LuaScriptException.class)
	public void testUpdateInvalidDataArgsThrowsException()
			throws LuaScriptException {
		String jid = addRecurringJob();

		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("update", jid, "data",
				"test-data");

		try {
			_luaScript.callScript(this.scriptName(), emptyValues, badValues);
		} catch (LuaScriptException ex) {
			throw ex;
		} finally {
			removeRecurringJob(jid);
		}
	}

	@Test(expected = LuaScriptException.class)
	public void testUpdateUnknownKeyArgsThrowsException()
			throws LuaScriptException {
		String jid = addRecurringJob();

		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("update", jid, "unknown-key",
				"unknown-value");

		try {
			testArgsException(emptyValues, badValues,
					"Unrecognized option \"unknown-key\"");
		} catch (LuaScriptException ex) {
			throw ex;
		} finally {
			removeRecurringJob(jid);
		}

	}

	@Test
	public void testUpdateUnknownJIDReturnsEmptyString()
			throws LuaScriptException {
		String jid = addNewTestJob();

		// Since this job is not recurring it simulates an unknown jid

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("update", jid);

		String result = (String) _luaScript.callScript(this.scriptName(), keys,
				args);

		assertEquals("", result);

		removeJob(jid);
	}

	@Test(expected = LuaScriptException.class)
	public void testTagMissingJIDArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("tag");

		testArgsException(emptyValues, badValues, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testUntagMissingJIDArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList("untag");

		testArgsException(emptyValues, badValues, "Arg \"jid\" missing");
	}

	@Test
	public void testCreateRecurringJob() throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);

		assertTrue(job.size() > 0);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_JOB, job.get("klass").toString());
		assertEquals(60, job.get("interval"));
		assertEquals(0, job.get("count"));
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		removeRecurringJob(jid);
	}

	@Test
	public void testGetRecurringJob() throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);

		assertTrue(job.size() > 0);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_JOB, job.get("klass").toString());
		assertEquals(60, job.get("interval"));
		assertEquals(0, job.get("count"));
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		removeRecurringJob(jid);
	}

	@Test
	public void testUpdateRecurringJob() throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);

		assertTrue(job.size() > 0);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_JOB, job.get("klass").toString());
		assertEquals(0, job.get("priority"));
		assertEquals(60, job.get("interval"));
		assertEquals(0, job.get("count"));
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("update", jid, "klass",
				"AnotherTestJob", "priority", "100");
		long result = (Long) _luaScript.callScript(this.scriptName(), keys,
				args);
		assertEquals(1, result);

		// Reget the job
		json = getRecurringJob(jid);
		job = JsonHelper.parseMap(json);
		assertEquals("AnotherTestJob", job.get("klass").toString());
		assertEquals(100, job.get("priority"));

		removeRecurringJob(jid);
	}

	@Test
	public void testUpdateRecurringJobFromQueue() throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("update", jid, "queue",
				"another-queue");
		long result = (Long) _luaScript.callScript(this.scriptName(), keys,
				args);
		assertEquals(1, result);

		// Reget the job
		json = getRecurringJob(jid);
		job = JsonHelper.parseMap(json);
		assertEquals("another-queue", job.get("queue").toString());

		// Test that this jid does not exist in test-queue, but does exist in
		// another-queue
		List<String> noKeys = new ArrayList<String>();
		List<String> queueArgs = Arrays.asList(
				JQlessClient.getCurrentSeconds(), TEST_QUEUE);

		json = (String) _luaScript.callScript("queues.lua", noKeys, queueArgs);
		Map<String, Object> queueDetails = JsonHelper.parseMap(json);
		for (String key : queueDetails.keySet()) {
			if (!key.equals("name")) {
				assertEquals(0, queueDetails.get(key));
			}
		}

		queueArgs = Arrays.asList(JQlessClient.getCurrentSeconds(),
				"another-queue");

		json = (String) _luaScript.callScript("queues.lua", noKeys, queueArgs);
		queueDetails = JsonHelper.parseMap(json);
		int totalJobs = 0;
		for (String key : queueDetails.keySet()) {
			if (!key.equals("name")) {
				totalJobs += (Integer) queueDetails.get(key);
			}
		}
		assertEquals(1, totalJobs);

		removeRecurringJob(jid);
	}

	@Test
	public void testAddTagsToRecurringJob() throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		// Create tags
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("tag", jid, "test-tag", "test-tag-2");
		json = (String) _luaScript.callScript(this.scriptName(), keys, args);
		List<String> tags = JsonHelper.parseList(json);
		assertTrue(tags.contains("test-tag"));
		assertTrue(tags.contains("test-tag-2"));

		removeRecurringJob(jid);
	}

	@Test
	public void testAddTagsToRecurringJobThatNoLongerExists()
			throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		// Create tags
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("tag", UUID.randomUUID().toString(),
				"test-tag");
		String result = (String) _luaScript.callScript(this.scriptName(), keys,
				args);

		assertEquals("", result);

		removeRecurringJob(jid);
	}

	@Test
	public void testRemoveTagsFromRecurringJob() throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		// Create tags
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("tag", jid, "test-tag", "test-tag-2");
		json = (String) _luaScript.callScript(this.scriptName(), keys, args);
		List<String> tags = JsonHelper.parseList(json);
		assertTrue(tags.contains("test-tag"));
		assertTrue(tags.contains("test-tag-2"));

		// Remove tags
		args = Arrays.asList("untag", jid, "test-tag-2");
		json = (String) _luaScript.callScript(this.scriptName(), keys, args);
		tags = JsonHelper.parseList(json);
		assertTrue(tags.contains("test-tag"));
		assertFalse(tags.contains("test-tag-2"));

		removeRecurringJob(jid);
	}

	@Test
	public void testRemoveTagsFromRecurringJobThatNoLongerExists()
			throws LuaScriptException {
		String jid = addRecurringJob();

		String json = getRecurringJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_QUEUE, job.get("queue").toString());
		assertEquals("recur", job.get("state").toString());

		// Create tags
		List<String> tagValues = Arrays.asList("test-tag", "test-tag-2");
		json = createJSON(tagValues);

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("untag",
				UUID.randomUUID().toString(), json);
		String result = (String) _luaScript.callScript(this.scriptName(), keys,
				args);

		assertEquals("", result);

		removeRecurringJob(jid);
	}
}
