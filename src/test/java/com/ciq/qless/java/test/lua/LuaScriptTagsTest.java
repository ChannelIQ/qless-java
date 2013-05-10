package com.ciq.qless.java.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.utils.JsonHelper;

public class LuaScriptTagsTest extends LuaScriptBaseTest {

	@Override
	protected String scriptName() {
		return "tag.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Tag";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "Got 1, expected 0");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingCommandArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();

		testArgsException(emptyValues, emptyValues,
				"Missing first arg \"add\", \"remove\" or \"get\"");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidCommandArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("bad-command");

		testArgsException(emptyValues, args,
				"First argument must be \"add\", \"remove\" or \"get\"");
	}

	@Test(expected = LuaScriptException.class)
	public void testAddCommandMissingJIDArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("add");

		testArgsException(emptyValues, args, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testAddCommandMissingNowArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("add", TEST_JID);

		testArgsException(emptyValues, args, "Arg \"now\" is not a number");
	}

	@Test(expected = LuaScriptException.class)
	public void testRemoveCommandMissingJIDArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("remove");

		testArgsException(emptyValues, args, "Arg \"jid\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testRemoveCommandMissingNowArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("remove", TEST_JID);

		testArgsException(emptyValues, args, "Arg \"now\" is not a number");
	}

	@Test(expected = LuaScriptException.class)
	public void testGetCommandMissingJIDArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> args = Arrays.asList("get");

		testArgsException(emptyValues, args, "Arg \"tag\" missing");
	}

	@Test
	public void testAddTagsToJob() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("add", jid,
				JQlessClient.getCurrentSeconds(), "test-tag");

		String tagJson = (String) _luaScript.callScript(this.scriptName(),
				keys, args);
		List<String> tags = JsonHelper.parseList(tagJson);
		assertTrue(tags.contains("test-tag"));

		String json = getJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);

		@SuppressWarnings("unchecked")
		List<String> jobTags = (List<String>) job.get("tags");
		assertTrue(jobTags.contains("test-tag"));

		removeJob(jid);
	}

	@Test
	public void testAddTagsForInvalidJob() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("add", UUID.randomUUID().toString(),
				JQlessClient.getCurrentSeconds(), "test-tag");

		String emptyString = (String) _luaScript.callScript(this.scriptName(),
				keys, args);

		assertEquals("", emptyString);

		removeJob(jid);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveTagsFromJob() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("add", jid,
				JQlessClient.getCurrentSeconds(), "test-tag", "test-tag-2");

		// Add the tags
		String tagJson = (String) _luaScript.callScript(this.scriptName(),
				keys, args);
		List<String> tags = JsonHelper.parseList(tagJson);
		assertTrue(tags.contains("test-tag"));
		assertTrue(tags.contains("test-tag-2"));

		// Get the job
		String json = getJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		List<String> jobTags = (List<String>) job.get("tags");
		assertTrue(jobTags.contains("test-tag"));
		assertTrue(jobTags.contains("test-tag-2"));

		// Remove the "test-tag-2" tag
		args = Arrays.asList("remove", jid, JQlessClient.getCurrentSeconds(),
				"test-tag-2");
		tagJson = (String) _luaScript.callScript(this.scriptName(), keys, args);
		tags = JsonHelper.parseList(tagJson);
		assertTrue(tags.contains("test-tag"));
		assertFalse(tags.contains("test-tag-2"));

		// Get the job
		json = getJob(jid);
		job = JsonHelper.parseMap(json);
		jobTags = (List<String>) job.get("tags");
		assertTrue(jobTags.contains("test-tag"));
		assertFalse(jobTags.contains("test-tag-2"));

		// Clean up
		removeJob(jid);
	}

	@Test
	public void testRemoveTagsFromInvalidJob() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("remove", UUID.randomUUID()
				.toString(), JQlessClient.getCurrentSeconds(), "test-tag");

		String emptyString = (String) _luaScript.callScript(this.scriptName(),
				keys, args);

		assertEquals("", emptyString);

		removeJob(jid);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetJobsByTag() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("add", jid,
				JQlessClient.getCurrentSeconds(), "test-tag", "test-tag-2");

		// Add the tags
		String tagJson = (String) _luaScript.callScript(this.scriptName(),
				keys, args);
		List<String> tags = JsonHelper.parseList(tagJson);
		assertTrue(tags.contains("test-tag"));
		assertTrue(tags.contains("test-tag-2"));

		// Get the job
		String json = getJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		List<String> jobTags = (List<String>) job.get("tags");
		assertTrue(jobTags.contains("test-tag"));
		assertTrue(jobTags.contains("test-tag-2"));

		// Remove the "test-tag-2" tag
		args = Arrays.asList("remove", jid, JQlessClient.getCurrentSeconds(),
				"test-tag-2");
		tagJson = (String) _luaScript.callScript(this.scriptName(), keys, args);
		tags = JsonHelper.parseList(tagJson);
		assertTrue(tags.contains("test-tag"));
		assertFalse(tags.contains("test-tag-2"));

		// Get the job
		args = Arrays.asList("get", "test-tag");
		tagJson = (String) _luaScript.callScript(this.scriptName(), keys, args);
		Map<String, Object> getResults = JsonHelper.parseMap(tagJson);
		List<String> jids = (List<String>) getResults.get("jobs");

		assertEquals("1", getResults.get("total").toString());
		assertTrue(jids.contains(jid));

		// Clean up
		removeJob(jid);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetJobsByUnknownTag() throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("add", jid,
				JQlessClient.getCurrentSeconds(), "test-tag", "test-tag-2");

		// Add the tags
		String tagJson = (String) _luaScript.callScript(this.scriptName(),
				keys, args);
		List<String> tags = JsonHelper.parseList(tagJson);
		assertTrue(tags.contains("test-tag"));
		assertTrue(tags.contains("test-tag-2"));

		// Get the job
		String json = getJob(jid);
		Map<String, Object> job = JsonHelper.parseMap(json);
		List<String> jobTags = (List<String>) job.get("tags");
		assertTrue(jobTags.contains("test-tag"));
		assertTrue(jobTags.contains("test-tag-2"));

		// Remove the "test-tag-2" tag
		args = Arrays.asList("remove", jid, JQlessClient.getCurrentSeconds(),
				"test-tag-2");
		tagJson = (String) _luaScript.callScript(this.scriptName(), keys, args);
		tags = JsonHelper.parseList(tagJson);
		assertTrue(tags.contains("test-tag"));
		assertFalse(tags.contains("test-tag-2"));

		// Get the job (and special handling for Lua Arrays
		args = Arrays.asList("get", "test-tag-2");
		tagJson = (String) _luaScript.callScript(this.scriptName(), keys, args);
		Map<String, Object> getResults = JsonHelper.parseMap(JsonHelper
				.fixArrayField(tagJson, "jobs"));
		List<String> jids = (List<String>) getResults.get("jobs");

		assertEquals("0", getResults.get("total").toString());
		assertFalse(jids.contains(jid));

		// Clean up
		removeJob(jid);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetJobsByTagsWithPaging() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());
		String jid3 = addJob(UUID.randomUUID().toString());
		String jid4 = addJob(UUID.randomUUID().toString());

		addTags(jid1, "common-tag", "test-tag", "test-tag-1");
		addTags(jid2, "common-tag", "test-tag", "test-tag-2");
		addTags(jid3, "common-tag", "test-tag-3");
		addTags(jid4, "common-tag", "test-tag-4");

		// First page
		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("get", "common-tag", "0", "2");
		String json = (String) _luaScript.callScript(this.scriptName(), keys,
				args);
		Map<String, Object> getResults = JsonHelper.parseMap(JsonHelper
				.fixArrayField(json, "jobs"));
		List<String> pageOne = (List<String>) getResults.get("jobs");

		assertEquals("4", getResults.get("total").toString());
		assertTrue(pageOne.size() == 2);

		// Second page
		args = Arrays.asList("get", "common-tag", "2", "2");
		json = (String) _luaScript.callScript(this.scriptName(), keys, args);
		getResults = JsonHelper
				.parseMap(JsonHelper.fixArrayField(json, "jobs"));
		List<String> pageTwo = (List<String>) getResults.get("jobs");

		assertEquals("4", getResults.get("total").toString());
		assertTrue(pageTwo.size() == 2);

		ArrayList<String> jids = new ArrayList<String>(pageOne);
		jids.addAll(pageTwo);

		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));
		assertTrue(jids.contains(jid3));
		assertTrue(jids.contains(jid4));

		// Cleanup
		removeJobs(jid1, jid2, jid3, jid4);
	}

	@Test
	public void testTopReturnsMostPopularTagsPaged() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob(UUID.randomUUID().toString());
		String jid3 = addJob(UUID.randomUUID().toString());
		String jid4 = addJob(UUID.randomUUID().toString());

		addTags(jid1, "common-tag", "test-tag", "test-tag-1");
		addTags(jid2, "common-tag", "test-tag", "test-tag-2");
		addTags(jid3, "common-tag", "test-tag-3");
		addTags(jid4, "common-tag", "test-tag-4");

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList("top", "0", "2");

		// Get the top results
		String json = (String) _luaScript.callScript(this.scriptName(), keys,
				args);
		List<String> tags = JsonHelper.parseList(json);
		assertTrue(tags.get(0).equals("common-tag"));
		assertTrue(tags.get(1).equals("test-tag"));
		assertFalse(tags.contains("test-tag-1"));

		args = Arrays.asList("top", "2", "4");

		// Get the top results
		json = (String) _luaScript.callScript(this.scriptName(), keys, args);
		tags = JsonHelper.parseList(json);

		assertTrue(tags.contains("test-tag-1"));
		assertTrue(tags.contains("test-tag-2"));
		assertTrue(tags.contains("test-tag-3"));
		assertTrue(tags.contains("test-tag-4"));

		// Cleanup
		removeJobs(jid1, jid2, jid3, jid4);
	}
}
