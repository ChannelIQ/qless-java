package com.ciq.jqless.test.lua;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.ciq.jqless.LuaScriptException;
import com.ciq.jqless.client.JQlessClient;

public class LuaScriptDependsTest extends LuaScriptTest {

	@Override
	protected String scriptName() {
		return "depends.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Depends";
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

		testArgsException(emptyValues, emptyValues, "Arg \"jid\" missing.");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID);

		testArgsException(emptyValues, badValues,
				"Arg \"now\" missing or not a number: nil");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidNowArgsThrowsException() throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID, "test-now");

		testArgsException(emptyValues, badValues,
				"Arg \"now\" missing or not a number: test-now");
	}

	@Test(expected = LuaScriptException.class)
	public void testMissingCommandArgsThrowsException()
			throws LuaScriptException {
		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(TEST_JID,
				JQlessClient.getCurrentSeconds());

		testArgsException(emptyValues, badValues, "Arg \"command\" missing");
	}

	@Test(expected = LuaScriptException.class)
	public void testInvalidCommandArgsThrowsException()
			throws LuaScriptException {
		String jid = addNewTestJob();

		List<String> emptyValues = new ArrayList<String>();
		List<String> badValues = Arrays.asList(jid,
				JQlessClient.getCurrentSeconds(), "bad-command");

		try {
			testArgsException(emptyValues, badValues,
					"Arg \"command\" must be \"on\" or \"off\"");
		} catch (LuaScriptException ex) {
			throw ex;
		} finally {
			removeJob(jid);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDependOnSingleJob() throws LuaScriptException {
		String jidParent = addNewTestJob();
		String jidChild = addJob(UUID.randomUUID().toString());

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jidParent,
				JQlessClient.getCurrentSeconds(), "on", jidChild);

		long results = (Long) _luaScript.callScript(this.scriptName(), noKeys,
				args);
		assertEquals(1, results);

		// Get Parent Job
		String json = getJob(jidParent);
		Map<String, Object> job = parseMap(json);
		assertEquals("depends", job.get("state"));

		// And Dependencies
		List<String> dependencies = (List<String>) job.get("dependencies");
		assertEquals(jidChild, dependencies.get(0).toString());

		// Get Child Job
		json = getJob(jidChild);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		List<String> depends = (List<String>) job.get("dependents");
		assertEquals(jidParent, depends.get(0).toString());

		removeJob(jidParent);
		removeJob(jidChild);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDependOnMultipleJobs() throws LuaScriptException {
		String jidParent = addNewTestJob();
		String jidChild = addJob(UUID.randomUUID().toString());
		String jidChild2 = addJob(UUID.randomUUID().toString());

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jidParent,
				JQlessClient.getCurrentSeconds(), "on", jidChild, jidChild2);

		long results = (Long) _luaScript.callScript(this.scriptName(), noKeys,
				args);
		assertEquals(1, results);

		// Get Parent Job
		String json = getJob(jidParent);
		Map<String, Object> job = parseMap(json);
		assertEquals("depends", job.get("state"));

		// And Dependencies
		List<String> dependencies = (List<String>) job.get("dependencies");
		assertTrue(dependencies.contains(jidChild));
		assertTrue(dependencies.contains(jidChild2));

		// Get First Child Job
		json = getJob(jidChild);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		List<String> depends = (List<String>) job.get("dependents");
		assertTrue(depends.contains(jidParent));

		// Get Second Child Job
		json = getJob(jidChild2);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.contains(jidParent));

		removeJob(jidParent);
		removeJob(jidChild);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveSingleDependency() throws LuaScriptException {
		String jidChild = addNewTestJob();
		List<String> dependencies = Arrays.asList(jidChild);

		String jidParent = addDependentJob(dependencies);

		// Get Parent Job
		String json = getJob(jidParent);
		Map<String, Object> job = parseMap(json);
		assertEquals("depends", job.get("state"));

		// And Dependencies
		dependencies = (List<String>) job.get("dependencies");
		assertTrue(dependencies.contains(jidChild));

		// Get First Child Job
		json = getJob(jidChild);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		List<String> depends = (List<String>) job.get("dependents");
		assertTrue(depends.contains(jidParent));

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jidParent,
				JQlessClient.getCurrentSeconds(), "off", jidChild);

		long results = (Long) _luaScript.callScript(this.scriptName(), noKeys,
				args);
		assertEquals(1, results);

		// Get Parent Job
		json = getJob(jidParent);
		json = fixArrayField(json, "dependencies");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Dependencies
		dependencies = (List<String>) job.get("dependencies");
		assertTrue(dependencies.size() == 0);

		// Get First Child Job
		json = getJob(jidChild);
		json = fixArrayField(json, "dependents");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.size() == 0);

		removeJob(jidParent);
		removeJob(jidChild);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveMultipleDependencies() throws LuaScriptException {
		String jidChild = addNewTestJob();
		String jidChild2 = addJob(UUID.randomUUID().toString());
		List<String> dependencies = Arrays.asList(jidChild, jidChild2);

		String jidParent = addDependentJob(dependencies);

		// Get Parent Job
		String json = getJob(jidParent);
		Map<String, Object> job = parseMap(json);
		assertEquals("depends", job.get("state"));

		// And Dependencies
		dependencies = (List<String>) job.get("dependencies");
		assertTrue(dependencies.contains(jidChild));
		assertTrue(dependencies.contains(jidChild2));

		// Get First Child Job
		json = getJob(jidChild);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		List<String> depends = (List<String>) job.get("dependents");
		assertTrue(depends.contains(jidParent));

		// Get Second Child Job
		json = getJob(jidChild2);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.contains(jidParent));

		// Stop the dependencies
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jidParent,
				JQlessClient.getCurrentSeconds(), "off", jidChild, jidChild2);

		long results = (Long) _luaScript.callScript(this.scriptName(), noKeys,
				args);
		assertEquals(1, results);

		// Get Parent Job
		json = getJob(jidParent);
		json = fixArrayField(json, "dependencies");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Dependencies
		dependencies = (List<String>) job.get("dependencies");
		assertTrue(dependencies.size() == 0);

		// Get First Child Job
		json = getJob(jidChild);
		json = fixArrayField(json, "dependents");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.size() == 0);

		// Get First Child Job
		json = getJob(jidChild);
		json = fixArrayField(json, "dependents");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.size() == 0);

		removeJob(jidParent);
		removeJob(jidChild);
		removeJob(jidChild2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveAllDependencies() throws LuaScriptException {
		String jidChild = addNewTestJob();
		String jidChild2 = addJob(UUID.randomUUID().toString());
		List<String> dependencies = Arrays.asList(jidChild, jidChild2);

		String jidParent = addDependentJob(dependencies);

		// Get Parent Job
		String json = getJob(jidParent);
		Map<String, Object> job = parseMap(json);
		assertEquals("depends", job.get("state"));

		// And Dependencies
		dependencies = (List<String>) job.get("dependencies");
		assertTrue(dependencies.contains(jidChild));
		assertTrue(dependencies.contains(jidChild2));

		// Get First Child Job
		json = getJob(jidChild);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		List<String> depends = (List<String>) job.get("dependents");
		assertTrue(depends.contains(jidParent));

		// Get Second Child Job
		json = getJob(jidChild2);
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.contains(jidParent));

		// Stop the dependencies
		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jidParent,
				JQlessClient.getCurrentSeconds(), "off", "all");

		long results = (Long) _luaScript.callScript(this.scriptName(), noKeys,
				args);
		assertEquals(1, results);

		// Get Parent Job
		json = getJob(jidParent);
		json = fixArrayField(json, "dependencies");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Dependencies
		dependencies = (List<String>) job.get("dependencies");
		assertTrue(dependencies.size() == 0);

		// Get First Child Job
		json = getJob(jidChild);
		json = fixArrayField(json, "dependents");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.size() == 0);

		// Get First Child Job
		json = getJob(jidChild);
		json = fixArrayField(json, "dependents");
		job = parseMap(json);
		assertEquals("waiting", job.get("state"));

		// And Depends
		depends = (List<String>) job.get("dependents");
		assertTrue(depends.size() == 0);

		removeJob(jidParent);
		removeJob(jidChild);
		removeJob(jidChild2);
	}

	@Test
	public void testRemoveDependencyForNonDependentJob()
			throws LuaScriptException {
		String jidParent = addNewTestJob();

		List<String> noKeys = new ArrayList<String>();
		List<String> args = Arrays.asList(jidParent,
				JQlessClient.getCurrentSeconds(), "off");

		String results = (String) _luaScript.callScript(this.scriptName(),
				noKeys, args);

		assertEquals("", results);

		removeJob(jidParent);
	}
}
