package com.ciq.qless.java.test.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.client.ClientJobs;
import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.test.BaseTest;

public class ClientJobsTest extends BaseTest {

	private static ClientJobs _clientJobs;

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		_clientJobs = new ClientJobs(new JQlessClient(jedis));
	}

	@Test
	public void testClientJobComplete() throws LuaScriptException {
		String jid1 = addNewTestJob();
		String jid2 = addJob();

		popJob();
		popJob();

		completeJob(jid1);
		completeJob(jid2);

		List<String> jids = _clientJobs.complete(0, 25);

		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));

		removeJobs(jid1, jid2);
	}

	@Test
	public void testClientJobTracked() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();

		popJob();
		popJob();

		trackJob(jid1);
		trackJob(jid2);

		List<BaseJob> jobs = _clientJobs.tracked();

		assertTrue(jobs.size() == 2);
		for (BaseJob job : jobs) {
			assertTrue(job.getAttributes().getJID().toString().equals(jid1)
					|| job.getAttributes().getJID().toString().equals(jid2));

		}

		removeJobs(jid1, jid2);
	}

	@Test
	public void testClientJobTagged() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();
		String jid3 = addJob();

		try {
			BaseJob job = _clientJobs.getJob(jid1);
			job.tag("test-tag", "test-tag-1");

			job = _clientJobs.getJob(jid2);
			job.tag("test-tag", "test-tag-2");

			job = _clientJobs.getJob(jid3);
			job.tag("test-tag", "test-tag-2");

			List<String> tagJids = _clientJobs.tagged("test-tag");
			assertTrue(tagJids.contains(jid1));
			assertTrue(tagJids.contains(jid2));
			assertTrue(tagJids.contains(jid3));

			List<String> tag1Jids = _clientJobs.tagged("test-tag-1");
			assertTrue(tag1Jids.contains(jid1));

			List<String> tag2Jids = _clientJobs.tagged("test-tag-2");
			assertTrue(tag2Jids.contains(jid2));
			assertTrue(tag2Jids.contains(jid3));
		} catch (Exception e) {
			fail("Job couldn't be found.  " + e.getMessage());
		} finally {
			removeJobs(jid1, jid2, jid3);
		}
	}

	@Test
	public void testClientJobFailed() throws LuaScriptException {
		String jid = addJob();

		popJob();

		String failedJID = failJob(jid, "test-group");
		assertTrue(jid.equals(failedJID));

		List<BaseJob> jobs = _clientJobs.failedByGroup("test-group");
		assertTrue(jobs.size() == 1);

		assertTrue(jobs.get(0).getAttributes().getJID().toString().equals(jid));

		removeJob(jid);
	}

	@Test
	public void testClientJobShowAllFailGroups() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();

		popJob();
		popJob();

		String jidFailed1 = failJob(jid1, "test-group");
		String jidFailed2 = failJob(jid2, "another-group");

		Map<String, Object> failures = _clientJobs.failed();
		assertTrue(failures.size() == 2);

		assertEquals(1, failures.get("test-group"));
		assertEquals(1, failures.get("another-group"));

		removeJobs(jid1, jid2);
	}

	@Test
	public void testClientJobGetJob() throws LuaScriptException {
		String jid = addJob();

		try {
			BaseJob j = _clientJobs.getJob(jid);
			assertTrue(j.getAttributes().getJID().toString().equals(jid));
		} catch (Exception ex) {
			fail("Could not find job: " + jid);
		} finally {
			removeJob(jid);
		}
	}

	@Test(expected = Exception.class)
	public void testClientJobGetUnknownJob() throws Exception {
		String jid = addJob();

		try {
			BaseJob j = _clientJobs.getJob(UUID.randomUUID().toString());
			fail("Job was found when it was expected to fail");
		} catch (Exception ex) {
			throw ex;
		} finally {
			removeJob(jid);
		}
	}

	@Test
	public void testClientJobGetRecurringJob() throws LuaScriptException {
		String jid = addRecurringJob();

		try {
			BaseJob job = _clientJobs.getJob(UUID.fromString(jid));
			assertTrue(job.getAttributes().getJID().toString().equals(jid));
		} catch (Exception ex) {
			fail("Could not find job: " + jid);
		} finally {
			removeRecurringJob(jid);
		}
	}
}
