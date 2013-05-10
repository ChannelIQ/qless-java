package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.test.BaseTest;

public abstract class LuaScriptTest extends BaseTest {
	private final String unpauseScript = "-- This script takes the name of the queue(s) and removes it"
			+ "-- from the ql:paused_queues set."
			+ "--"
			+ "-- Args: The list of queues to pause."
			+ "if #KEYS > 0 then error('Pause(): No Keys should be provided') end"
			+ "if #ARGV < 1 then error('Pause(): Must provide at least one queue to pause') end"
			+ "local key = 'ql:paused_queues'"
			+ "redis.call('srem', key, unpack(ARGV));";

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis = null;
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

}
