package com.ciq.jqless.test.lua;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.jqless.LuaScript;
import com.ciq.jqless.LuaScriptException;
import com.ciq.jqless.client.JQlessClient;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class LuaScriptTest {
	private final String unpauseScript = "-- This script takes the name of the queue(s) and removes it"
			+ "-- from the ql:paused_queues set."
			+ "--"
			+ "-- Args: The list of queues to pause."
			+ "if #KEYS > 0 then error('Pause(): No Keys should be provided') end"
			+ "if #ARGV < 1 then error('Pause(): Must provide at least one queue to pause') end"
			+ "local key = 'ql:paused_queues'"
			+ "redis.call('srem', key, unpack(ARGV));";
	protected final String TEST_JID = "d1ecbfa0-48d7-47fd-affd-f96a75e46216";
	protected final String TEST_QUEUE = "test-queue";
	protected final String TEST_WORKER = "test-worker";

	protected Jedis _jedis = null;
	protected LuaScript _luaScript = null;

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis = null;
	}

	@Before
	public void initialize() {
		_jedis = new Jedis("localhost");
		_luaScript = new LuaScript(_jedis);
	}

	@Test
	@Ignore
	public void luaScriptLoad() {
		String contents = _luaScript.getScript("unpause.lua");
		assertEquals(contents, unpauseScript);
	}

	@After
	public void teardown() {
		// try {
		// // removeJob(TEST_JID);
		// } catch (LuaScriptException e) {
		// // Log errors
		// System.out.println("Exception: " + e.getMessage());
		// }

		_jedis = null;
	}

	protected abstract String scriptName();

	protected abstract String scriptErrorName();

	/*
	 * Common Script Functions
	 */
	protected void testArgsException(List<String> keys, List<String> args,
			String errorMsg) throws LuaScriptException {
		try {
			_luaScript.callScript(this.scriptName(), keys, args);
		} catch (LuaScriptException ex) {
			System.out.println(ex.getMessage());
			assertEquals(ex.getMessage(), errorMsg);
			assertEquals(ex.getMethod(), this.scriptErrorName());
			throw ex;
		}
	}

	/*
	 * Job Building Section
	 */

	protected List<String> newJobArgsBuilder(String jid) {
		return Arrays.asList(jid, "SimpleTestJob", "{}",
				JQlessClient.getCurrentSeconds(), "0");
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
		List<String> args = Arrays.asList(parentJID, "SimpleTestJob", "{}",
				JQlessClient.getCurrentSeconds(), "0", "depends", output);

		return addJob(keys, args);
	}

	protected String addRecurringJob() throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("on", TEST_QUEUE, TEST_JID,
				"SimpleTestJob", "{}", JQlessClient.getCurrentSeconds(),
				"interval", "60", "0");

		String jid = (String) _luaScript.callScript("recur.lua", keys, args);

		return jid;
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

	protected String removeJobs(final List<String> jids)
			throws LuaScriptException {
		List<String> keys = new ArrayList<String>();

		try {
			Object o = _luaScript.callScript("cancel.lua", keys, jids);
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

	/*
	 * JSON Engine
	 */

	@SuppressWarnings("unchecked")
	protected <E> List<E> parseList(String json) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		List<E> result = null;
		try {
			jp = factory.createParser(json);
			result = jp.readValueAs(List.class);
		} catch (JsonParseException e) {
			System.out.println("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			System.out.println("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}

		return result;
	}

	protected <E, T> List<T> parseList(List<String> input, Class<E> clazz,
			Class<T> innerClazz) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		List<T> result = new ArrayList<T>();
		try {
			for (String json : input) {
				jp = factory.createParser(json);
				result.add(jp.readValueAs(innerClazz));
			}
		} catch (JsonParseException e) {
			System.out.println("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			System.out.println("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> parseMap(String json) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		Map<String, Object> result = null;
		try {
			jp = factory.createParser(json);
			result = jp.readValueAs(HashMap.class);
		} catch (JsonParseException e) {
			System.out.println("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			System.out.println("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> parseMapFirstObject(List<String> scriptResult) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		Map<String, Object> result = null;
		try {
			jp = factory.createParser(scriptResult.get(0));
			result = jp.readValueAs(HashMap.class);
		} catch (JsonParseException e) {
			System.out.println("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			System.out.println("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}

		return result;
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

	/*
	 * JSON Helper Methods/Converters
	 */
	protected String fixArrayField(String json, String field) {
		return json.replace("\"" + field + "\":{}", "\"" + field + "\":[]");
	}
}
