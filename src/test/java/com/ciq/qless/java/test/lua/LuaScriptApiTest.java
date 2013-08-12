package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;

public class LuaScriptApiTest extends LuaScriptBaseTest {

	@Override
	protected String scriptName() {
		return "qless.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "QlessApi";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testCommandMustBeProvidedThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Must provide a command");
	}

	@Test(expected = LuaScriptException.class)
	public void testUnknownCommandThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> unknownCommand = Arrays.asList("unknown-command");

		testArgsException(emptyValues, unknownCommand,
				"Unknown command unknown-command");
	}

	@Test(expected = LuaScriptException.class)
	public void testNowCommandMissingThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> unknownCommand = Arrays.asList("config.get");

		testArgsException(emptyValues, unknownCommand,
				"Arg 'now' missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testNowCommandNotANumberThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> unknownCommand = Arrays.asList("unknown-command");

		testArgsException(emptyValues, unknownCommand,
				"Unknown command unknown-command");
	}

	/*
	 * CONFIG SECTION
	 */

	@Test
	public void testGetDefaultConfig() {
		long HEARTBEAT = 60;
		List<String> args = Arrays.asList("heartbeat",
				String.valueOf(HEARTBEAT));

		try {
			List<String> getArgs = Arrays.asList("heartbeat");

			Object o = callLuaScript("config.get", getArgs);

			assertEquals(HEARTBEAT, o);
		} catch (Exception e1) {
			System.out.println("Exception: " + e1.getMessage());
			fail(e1.getMessage());
		}
	}

	@Test
	public void testSetThenGetConfig() {
		long HEARTBEAT = 30;
		List<String> args = Arrays.asList("heartbeat",
				String.valueOf(HEARTBEAT));

		String scriptResult = "";
		try {
			callLuaScript("config.set", args);

			List<String> getArgs = Arrays.asList("heartbeat");

			scriptResult = (String) callLuaScript("config.get", getArgs);

			assertEquals(String.valueOf(HEARTBEAT), scriptResult);
		} catch (Exception e1) {
			System.out.println("Exception: " + e1.getMessage());
			fail(e1.getMessage());
		}
	}

	@Test
	public void testSetThenUnsetThenGetConfig() {
		long HEARTBEAT = 30;
		List<String> args = Arrays.asList("heartbeat",
				String.valueOf(HEARTBEAT));

		try {
			callLuaScript("config.set", args);

			args = Arrays.asList("heartbeat");
			// Unset
			callLuaScript("config.unset", args);

			// Get
			Object o = callLuaScript("config.get", args);

			assertEquals("60", String.valueOf(o));
		} catch (Exception e1) {
			System.out.println("Exception: " + e1.getMessage());
			fail(e1.getMessage());
		}
	}

	private Object callLuaScript(String command, List<String> args)
			throws LuaScriptException {
		ArrayList<String> callArgs = new ArrayList<String>(args);

		callArgs.add(0, JQlessClient.getCurrentSeconds());
		callArgs.add(0, command);

		return _luaScript.callScript(this.scriptName(),
				new ArrayList<String>(), callArgs);
	}
}
