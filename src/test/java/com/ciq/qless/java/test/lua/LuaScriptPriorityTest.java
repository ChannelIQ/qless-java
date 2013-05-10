package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.utils.JsonHelper;

public class LuaScriptPriorityTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "priority.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Priority";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "Got 1, expected 0");
	}

	@Test
	public void testSettingPriorityForUnknownJob() throws LuaScriptException {
		String jid = addNewTestJob();

		String json = getJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals(jid, job.get("jid").toString());

		long orignalPriority = Long.valueOf(job.get("priority").toString());
		long setPriority = Long.valueOf(orignalPriority) + 100;

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(UUID.randomUUID().toString(),
				String.valueOf(setPriority));

		String notFound = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertEquals("", notFound);

		removeJob(jid);
	}

	@Test
	public void testSettingPriorityForJob() throws LuaScriptException {
		String jid = addNewTestJob();

		String json = getJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		assertEquals(jid, job.get("jid").toString());

		long orignalPriority = Long.valueOf(job.get("priority").toString());
		long setPriority = Long.valueOf(orignalPriority) + 100;

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid, String.valueOf(setPriority));

		long newPriority = (Long) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertFalse(orignalPriority == newPriority);
		assertEquals(setPriority, newPriority);

		removeJob(jid);
	}
}
