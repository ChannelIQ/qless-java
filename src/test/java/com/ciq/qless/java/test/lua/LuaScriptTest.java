package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import org.junit.Test;

public class LuaScriptTest extends LuaScriptBaseTest {
	private final String unpauseScript = "-- This script takes the name of the queue(s) and removes it"
			+ "-- from the ql:paused_queues set."
			+ "--"
			+ "-- Args: The list of queues to pause."
			+ "if #KEYS > 0 then error('Pause(): No Keys should be provided') end"
			+ "if #ARGV < 1 then error('Pause(): Must provide at least one queue to pause') end"
			+ "local key = 'ql:paused_queues'"
			+ "redis.call('srem', key, unpack(ARGV));";

	@Test
	@Ignore
	public void luaScriptLoad() {
		String contents = _luaScript.getScript(this.scriptName());
		assertEquals(contents, unpauseScript);
	}

	@Override
	protected String scriptName() {
		return "unpause.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Unpause";
	}
}
