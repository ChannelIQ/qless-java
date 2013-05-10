package com.ciq.qless.java.test.lua;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.lua.LuaScriptException;

public class LuaScriptCancelTest extends LuaScriptBaseTest {

	@Override
	protected String scriptName() {
		return "cancel.lua";
	}

	@Override
	protected String scriptErrorName() {
		return "Cancel";
	}

	@Test(expected = LuaScriptException.class)
	public void testKeysShouldBeEmptyThrowsException()
			throws LuaScriptException {
		List<String> badValues = Arrays.asList("non-empty-key");

		testArgsException(badValues, badValues, "No Keys should be provided");
	}

	@Test
	public void testCancelSingleJob() throws Exception {
		String jid = addNewTestJob();

		String result = removeJob(jid);

		result = getJob(jid);

		assertEquals(result, "");
	}

	@Test
	public void testCancelMultipleJobs() throws Exception {
		String jid1 = addNewTestJob();

		String jid2 = UUID.randomUUID().toString();
		addJob(jid2);

		String result = getJob(jid1);
		assertThat(result, not(""));

		result = getJob(jid2);
		assertThat(result, not(""));

		removeJobs(jid1, jid2);

		result = getJob(jid1);
		assertEquals(result, "");

		result = getJob(jid2);
		assertEquals(result, "");
	}

	@Test(expected = LuaScriptException.class)
	public void testCancelDependentJobMissingException() throws Exception {
		String dependentJID = addNewTestJob();
		String parentJID = UUID.randomUUID().toString();

		String output = createJSON(Arrays.asList(dependentJID));
		List<String> keys = Arrays.asList(TEST_QUEUE);
		List<String> args = Arrays.asList(parentJID, "SimpleTestJob", "{}",
				JQlessClient.getCurrentSeconds(), "0", "depends", output);

		addJob(keys, args);

		String result = getJob(dependentJID);

		String result2 = getJob(parentJID);

		try {
			String removeResult = removeJob(dependentJID);
			fail("Expected LuaScriptException");
		} catch (LuaScriptException ex) {
			System.out.println(ex.getMessage());
			throw ex;
		} finally {
			removeJobs(parentJID, dependentJID);
		}
	}

	@Test
	public void testCancelDependentJob() throws LuaScriptException {
		String dependentJID = addNewTestJob();
		List<String> dependencies = Arrays.asList(dependentJID);

		String parentJID = addDependentJob(dependencies);

		String result = getJob(dependentJID);
		assertThat(result, not(""));

		String result2 = getJob(parentJID);
		assertThat(result2, not(""));

		String removeResult = removeJobs(parentJID, dependentJID);
		assertEquals(removeResult, "");
	}
}
