package com.ciq.qless.java.test.lua;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ciq.qless.java.lua.LuaScriptException;

public class LuaScriptDeregisterWorkersTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "deregister_workers.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "DeregisterWorkers";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testAtLeastOneArgumentMustBeProvided()
			throws LuaScriptException {
		List<String> noValues = new ArrayList<String>();

		testArgsException(noValues, noValues,
				"Must provide at least one worker to deregister");
	}
}
