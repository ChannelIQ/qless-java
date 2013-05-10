package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.utils.JsonHelper;

public class LuaScriptGetTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "get.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Get";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingJIDArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues, "Arg \"jid\" missing");
	}

	@Test
	public void testGetSuccessful() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(jid);

		String json = (String) _luaScript.callScript(this.scriptName(), keys,
				args);

		System.out.println(json);

		Map<String, Object> job = JsonHelper.parseMap(json);

		assertEquals(jid, job.get("jid").toString());
		assertEquals(TEST_JOB, job.get("klass").toString());
		assertEquals("waiting", job.get("state").toString());

		removeJob(jid);
	}

	@Test
	public void testGetFails() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(UUID.randomUUID().toString());

		String response = (String) _luaScript.callScript(this.scriptName(),
				keys, args);

		assertEquals("", response);

		removeJob(jid);
	}
}
