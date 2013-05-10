package com.ciq.qless.java.test.lua;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ciq.qless.java.lua.LuaScriptException;

public class LuaScriptUnpauseTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "unpause.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Pause";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testAtLeastOneArgumentMustBePassed() throws LuaScriptException {
		List<String> noValue = new ArrayList<String>();

		testArgsException(noValue, noValue,
				"Must provide at least one queue to pause");
	}

}
