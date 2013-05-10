package com.ciq.qless.java.test.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.test.BaseTest;

public class SimpleTestJobTest extends BaseTest {

	private String _jid;
	private BaseJob _job;
	private JQlessClient _client;

	@Before
	public void setupJob() throws Exception {
		_jid = addNewTestJob();
		_client = new JQlessClient(this._jedis);
		_job = _client.Jobs().getJob(_jid);
	}

	@After
	public void closeJob() throws LuaScriptException {
		removeJob(_jid);
	}

	@Test
	public void testClassDescription() {
		assertEquals(String.format(
				"com.ciq.qless.java.test.jobs.SimpleTestJob (%s / %s / %s)",
				_jid, TEST_QUEUE, "waiting"), _job.getDescription());
	}

	@Test
	public void testIsStateChanged() {
		assertTrue(_job.isStateChanged() == false);
	}

	@Test
	public void testIsStateChangedAfterChange() {
		// move this job to a new queue
		_job.move("another-queue");

		assertTrue(_job.isStateChanged() == true);
	}

	@Test
	@Ignore
	public void testGetTimeToLive() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetPriority() {
		_job.setPriority(100);

		assertEquals(100, _job.getAttributes().getPriority());

		try {
			_job = _client.Jobs().getJob(_jid);

			assertEquals(100, _job.getAttributes().getPriority());
		} catch (Exception e) {
			fail("Could not find job: " + _jid);
		}
	}

	@Test
	@Ignore
	public void testQueueHistory() {
		fail("Not yet implemented");
	}

	@Test
	@Ignore
	public void testGetFirstOccurence() {
		// / Create a job and take note of the timestamp. Complete it. Create
		// the same job. Complete it.
		// Compare the timestamp to the lowest timestamp
		fail("Not yet implemented");
	}

	@Test
	@Ignore
	public void testGetHash() {
		Map<String, Object> hash = _job.getHash();
		assertEquals(_jid, hash.get("jid").toString());
		fail("More details needed");
	}

	@Test
	public void testMoveToNewQueue() throws Exception {
		// move this job to a new queue
		_job.move("another-queue");

		_job = _client.Jobs().getJob(_jid);

		assertEquals("another-queue", _job.getQueue().name());
	}

	@Test
	public void testFailANonRunningJob() throws Exception {
		String empty = _job.fail("Epic Failure", "This is an epic problem");
		assertEquals("", empty);
	}

	@Test
	public void testFailAJob() throws Exception {
		popJobLocal(TEST_QUEUE);

		String jid = _job.fail("Epic Failure", "This is an epic problem");
		assertEquals(_jid, jid);

		_job = _client.Jobs().getJob(_jid);
		assertEquals("failed", _job.getAttributes().getState());
		assertTrue(_job.getAttributes().getFailure().containsKey("group"));
		assertEquals("Epic Failure",
				_job.getAttributes().getFailure().get("group").toString());
		assertTrue(_job.getAttributes().getFailure().containsKey("message"));
		assertEquals("This is an epic problem", _job.getAttributes()
				.getFailure().get("message").toString());
	}

	@Test
	public void testHeartbeatJob() throws Exception {
		long timestamp = _job.heartbeat();

		_job = _client.Jobs().getJob(_jid);

		assertEquals(new LocalDateTime(timestamp, DateTimeZone.UTC)
				.toDateTime().getMillis(), _job.getAttributes().getExpiresAt()
				.toDateTime().getMillis());
	}

	@Test
	public void testCompleteNonRunningJob() throws Exception {
		String status = _job.complete();

		_job = _client.Jobs().getJob(_jid);

		assertEquals("waiting", _job.getAttributes().getState());
	}

	@Test
	public void testCompleteJob() throws Exception {
		popJobLocal(TEST_QUEUE);

		String status = _job.complete();

		assertEquals("complete", status);
	}

	@Test
	public void testCompleteAndRescheduleJob() throws Exception {
		popJobLocal(TEST_QUEUE);

		String status = _job.complete("next-queue", 0);

		_job = _client.Jobs().getJob(_jid);
		assertEquals("waiting", status);
		assertEquals("next-queue", _job.getAttributes().getQueueName());
	}

	@Test(expected = Exception.class)
	public void testCancelJob() throws Exception {
		_job.cancel();

		_job = _client.Jobs().getJob(_jid);

		fail("We expected the job to not exist any longer");
	}

	@Test
	public void testTrackJob() {
		boolean success = _job.track();

		assertEquals(true, success);
	}

	@Test
	public void testUntrackJob() {
		boolean success = _job.untrack();

		assertEquals(true, success);
	}

	@Test
	public void testTagJob() {
		List<String> tags = _job.tag("test-tag");
		assertTrue(tags.contains("test-tag"));

		tags = _job.tag("test-tag-2");
		assertTrue(tags.contains("test-tag"));
		assertTrue(tags.contains("test-tag-2"));
	}

	@Test
	public void testUntagJob() {
		List<String> tags = _job.tag("test-tag", "test-tag-2");
		assertTrue(tags.contains("test-tag"));
		assertTrue(tags.contains("test-tag-2"));

		tags = _job.untag("test-tag-2");
		assertTrue(tags.contains("test-tag"));
		assertFalse(tags.contains("test-tag-2"));
	}

	@Test
	public void testRetryJob() throws LuaScriptException {
		popJobLocal(TEST_QUEUE);

		long retriesRemaining = _job.retry();

		assertEquals(4, retriesRemaining);

		long notReady = _job.retry();
		assertEquals(0, notReady);

		popJobLocal(TEST_QUEUE);

		retriesRemaining = _job.retry();

		assertEquals(3, retriesRemaining);
	}

	@Test
	public void testJobDepends() throws Exception {
		String jid1 = addJob();
		String jid2 = addJob();

		boolean success = _job.depend(jid1, jid2);
		assertEquals(true, success);

		_job = _client.Jobs().getJob(_jid);
		assertEquals(2, _job.getAttributes().getDependencies().size());
		assertEquals("depends", _job.getAttributes().getState());

		BaseJob dependJob = _client.Jobs().getJob(jid1);
		assertEquals(1, dependJob.getAttributes().getDependents().size());
	}

	@Test
	public void testJobUndepends() throws Exception {
		String jid1 = addJob();
		String jid2 = addJob();

		boolean success = _job.depend(jid1, jid2);
		assertEquals(true, success);

		_job = _client.Jobs().getJob(_jid);
		assertEquals(2, _job.getAttributes().getDependencies().size());
		assertEquals("depends", _job.getAttributes().getState());

		BaseJob dependJob = _client.Jobs().getJob(jid1);
		assertEquals(1, dependJob.getAttributes().getDependents().size());

		_job.undepend(jid1);

		_job = _client.Jobs().getJob(_jid);
		assertEquals(1, _job.getAttributes().getDependencies().size());
		assertEquals("depends", _job.getAttributes().getState());

		dependJob = _client.Jobs().getJob(jid1);
		assertEquals(0, dependJob.getAttributes().getDependents().size());
	}

	@Test
	@Ignore
	public void testNoteStateChanged() {
		fail("Not yet implemented");
	}

	@Test
	public void testWorkWasPerformed() throws Exception {
		popJobLocal(TEST_QUEUE);

		_job.performWork();

		_job = _client.Jobs().getJob(_jid);

		assertEquals("complete", _job.getAttributes().getState());
	}

	private void popJobLocal(String queueName) {
		Queue q = _client.Queues().getNamedQueue(queueName);
		_job = q.pop();
		assertEquals(JQlessClient.getMachineName(), _job.getAttributes()
				.getWorkerName());
		assertEquals("running", _job.getAttributes().getState());
	}
}
