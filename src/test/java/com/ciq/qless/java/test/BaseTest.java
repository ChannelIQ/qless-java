package com.ciq.qless.java.test;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Before;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScript;
import com.ciq.qless.java.lua.LuaScriptException;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class BaseTest {
	protected final String TEST_JID = "d1ecbfa0-48d7-47fd-affd-f96a75e46216";
	protected final String TEST_QUEUE = "test-queue";
	protected final String TEST_WORKER = "test-worker";
	protected final String TEST_JOB = "com.ciq.qless.java.test.jobs.SimpleTestJob";

	protected Jedis _jedis = null;
	protected LuaScript _luaScript = null;

	@Before
	public void initialize() {
		_jedis = new Jedis("localhost");
		_jedis.flushAll();
		_luaScript = new LuaScript(_jedis);
	}

	/*
	 * Job Building Section
	 */

	protected List<String> newJobArgsBuilder(String jid) {
		return Arrays.asList(jid, TEST_JOB, "{}",
				JQlessClient.getCurrentSeconds(), "0");
	}

	protected String addJob() throws LuaScriptException {
		return addJob(UUID.randomUUID().toString());
	}

	protected String addNewTestJob() throws LuaScriptException {
		return addJob(TEST_JID);
	}

	protected String addJob(String jid) throws LuaScriptException {
		return addJob(jid, TEST_QUEUE);
	}

	protected String addJob(String jid, String queue) throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		keys.add(queue);

		List<String> args = newJobArgsBuilder(jid);

		return addJob(keys, args);
	}

	protected String addJob(List<String> keys, List<String> args)
			throws LuaScriptException {
		String scriptResult = "";
		try {
			scriptResult = (String) _luaScript
					.callScript("put.lua", keys, args);
		} catch (LuaScriptException e1) {
			System.out.println("Exception: " + e1.getMessage());
			throw e1;
		}

		return scriptResult;
	}

	protected String addDependentJob(List<String> jids)
			throws LuaScriptException {
		String parentJID = UUID.randomUUID().toString();

		String output = createJSON(jids);
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(parentJID, TEST_JOB, "{}",
				JQlessClient.getCurrentSeconds(), "0", "depends", output);

		return addJob(keys, args);
	}

	protected String addRecurringJob() throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("on", TEST_QUEUE, UUID.randomUUID()
				.toString(), TEST_JOB, "{}", JQlessClient.getCurrentSeconds(),
				"interval", "60", "0");

		String jid = (String) _luaScript.callScript("recur.lua", keys, args);

		return jid;
	}

	protected String addDelayedJob(int delay) throws LuaScriptException {
		String parentJID = UUID.randomUUID().toString();

		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(parentJID, TEST_JOB, "{}",
				JQlessClient.getCurrentSeconds(), String.valueOf(delay));

		return addJob(keys, args);
	}

	protected String getRecurringJob(String jid) throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("get", jid);

		String json = (String) _luaScript.callScript("recur.lua", keys, args);

		return json;
	}

	protected boolean removeRecurringJob(String jid) throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("off", jid);

		long result = (Long) _luaScript.callScript("recur.lua", keys, args);

		return result == 1 ? true : false;
	}

	protected String getJob(String jid) throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid);

		try {
			return (String) _luaScript.callScript("get.lua", keys, args);
		} catch (LuaScriptException e1) {
			System.out.println("Exception: " + e1.getMessage());
			throw e1;
		}
	}

	protected String removeJob(String jid) throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid);

		try {
			Object o = _luaScript.callScript("cancel.lua", keys, args);
			return (String) o;
		} catch (LuaScriptException e1) {
			System.out.println("Exception: " + e1.getMessage());
			throw e1;
		}
	}

	protected String removeJobs(String... jids) throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		ArrayList<String> ids = new ArrayList<String>();
		for (String jid : jids) {
			ids.add(jid);
		}

		try {
			Object o = _luaScript.callScript("cancel.lua", keys, ids);
			return (String) o;
		} catch (LuaScriptException e1) {
			System.out.println("Exception: " + e1.getMessage());
			throw e1;
		}
	}

	protected List<String> popJob() throws LuaScriptException {
		return popJob(TEST_WORKER);
	}

	@SuppressWarnings("unchecked")
	protected List<String> popJob(String worker) throws LuaScriptException {
		try {
			List<String> keys = Arrays.asList(TEST_QUEUE);
			List<String> getArgs = Arrays.asList(worker, "1",
					JQlessClient.getCurrentSeconds());
			return (List<String>) _luaScript.callScript("pop.lua", keys,
					getArgs);
		} catch (LuaScriptException e1) {
			System.out.println("Exception: " + e1.getMessage());
			throw e1;
		}
	}

	protected String completeJob(String jid) throws LuaScriptException {
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, TEST_WORKER, TEST_QUEUE,
				JQlessClient.getCurrentSeconds(), "{}");

		String result = (String) _luaScript.callScript("complete.lua", noKeys,
				args);

		return result;
	}

	protected String failJob(String jid, String group)
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> failArgs = Arrays.asList(jid, TEST_WORKER, group,
				"test-message", JQlessClient.getCurrentSeconds());

		String failedJID = (String) _luaScript.callScript("fail.lua",
				emptyValues, failArgs);

		assertEquals(jid, failedJID);

		return failedJID;
	}

	protected void addTags(String jid, String... tags)
			throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		ArrayList<String> args = new ArrayList<String>(Arrays.asList("add",
				jid, JQlessClient.getCurrentSeconds()));

		for (String tag : tags) {
			args.add(tag);
		}

		_luaScript.callScript("tag.lua", keys, args);
	}

	protected void trackJob(String jid) throws LuaScriptException {
		performTrackingAction("track", jid);
	}

	protected void untrackAndRemoveJob(String jid) throws LuaScriptException {
		performTrackingAction("untrack", jid);
		removeJob(jid);
	}

	private void performTrackingAction(String action, String jid)
			throws LuaScriptException {
		List<String> emptyKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(action, jid,
				JQlessClient.getCurrentSeconds());
		long result = (Long) _luaScript
				.callScript("track.lua", emptyKeys, args);

		assertEquals(1, result);
	}

	protected String createJSON(List<String> values) {
		final OutputStream out = new ByteArrayOutputStream();
		final ObjectMapper mapper = new ObjectMapper();

		String output = "";
		try {
			mapper.writeValue(out, values);

			final byte[] data = ((ByteArrayOutputStream) out).toByteArray();
			output = new String(data);
			System.out.println(output);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return output;
	}
}
