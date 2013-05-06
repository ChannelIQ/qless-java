package com.ciq.jqless.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.ciq.jqless.LuaScriptException;

public class LuaScriptConfigTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "config.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Config";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testSetWithoutOptionThrowsException() throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("set");

		testArgsException(keys, args, "Arg \"option\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testSetWithoutValueThrowsException() throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("set", "heartbeat");

		testArgsException(keys, args, "Arg \"value\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testUnsetWithoutOptionThrowsException()
			throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("unset");

		testArgsException(keys, args, "Arg \"option\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testUnknownCommandThrowsException() throws LuaScriptException {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("badcommand");

		testArgsException(keys, args, "Unrecognized command badcommand");
	}

	@Test
	public void testGetDefaults() {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("get");

		String scriptResult = "";
		try {
			scriptResult = (String) _luaScript.callScript("config.lua", keys,
					args);
		} catch (LuaScriptException e1) {
			System.out.println("Exception: " + e1.getMessage());
			fail(e1.getMessage());
		}

		Map<String, Object> result = parseMap(scriptResult);

		for (String key : result.keySet()) {
			if (key.equals("application")) {
				assertEquals("qless", result.get(key));
			}

			if (key.equals("heartbeat")) {
				assertEquals(60, result.get(key));
			}
		}
	}

	@Test
	public void testSetThenGet() {
		int HEARTBEAT = 30;
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("set", "heartbeat",
				String.valueOf(HEARTBEAT));

		String scriptResult = "";
		try {
			scriptResult = (String) _luaScript.callScript("config.lua", keys,
					args);

			List<String> getArgs = Arrays.asList("get");

			scriptResult = (String) _luaScript.callScript("config.lua", keys,
					getArgs);
		} catch (Exception e1) {
			System.out.println("Exception: " + e1.getMessage());
			fail(e1.getMessage());
		}

		Map<String, Object> result = parseMap(scriptResult);

		for (String key : result.keySet()) {
			if (key.equals("heartbeat")) {
				assertEquals(String.valueOf(HEARTBEAT), result.get(key));
			}
		}
	}

	@Test
	public void testUnsetThenTestDefaults() {
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("unset", "heartbeat");

		String scriptResult = "";
		try {
			scriptResult = (String) _luaScript.callScript("config.lua", keys,
					args);

			List<String> getArgs = Arrays.asList("get");

			scriptResult = (String) _luaScript.callScript("config.lua", keys,
					getArgs);
		} catch (Exception e1) {
			System.out.println("Exception: " + e1.getMessage());
			fail(e1.getMessage());
		}

		Map<String, Object> result = parseMap(scriptResult);

		for (String key : result.keySet()) {
			if (key.equals("application")) {
				assertEquals("qless", result.get(key));
			}

			if (key.equals("heartbeat")) {
				assertEquals(60, result.get(key));
			}
		}
	}
}
