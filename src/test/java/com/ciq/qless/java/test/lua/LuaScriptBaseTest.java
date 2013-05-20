package com.ciq.qless.java.test.lua;

import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.test.BaseTest;

public abstract class LuaScriptBaseTest extends BaseTest {

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis = null;
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
			// assertEquals(ex.getMessage(), errorMsg);
			// assertEquals(ex.getMethod(), this.scriptErrorName());
			throw ex;
		}
	}

}
