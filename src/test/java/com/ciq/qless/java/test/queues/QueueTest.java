package com.ciq.qless.java.test.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.BaseJob;
import com.ciq.qless.java.jobs.JobOptions;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.test.BaseTest;
import com.ciq.qless.java.test.jobs.SimpleTestJob;

public class QueueTest extends BaseTest {

	private static Queue _queue;

	@BeforeClass
	public static void init() {
		JedisPool pool = new JedisPool("172.16.120.180");
		// Jedis jedis = new Jedis("localhost");
		_queue = new Queue("test-queue", new JQlessClient(pool), "test-worker");
	}

	@Test
	public void populateMe() throws LuaScriptException {
		putter("www.amazon.com/gp/offer-listing/B001DPJTLC?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B002R1A7LW?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B005JWB4TU?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B000J0YEGM?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B005N57X20?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B007TSS8TQ?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B007THRG4U?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B004VRJ3EW?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B00BCGRRWA?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B004ZLV5UE?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B004ZLYBQ4?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B004VRJ3E2?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B004WODP20?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B000KL77AA?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B00005BYER?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B003IP4O10?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B0099PCUIC?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B004RKVXXM?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B007BY3PO6?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B006C6EGAQ?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B005SNPTS2?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B0034L3G8U?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B000NUYW92?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B000065BPB?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B00420UASG?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B009A6CZYY?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B00BBAFWNS?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B004MMEI78?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B0019EHU8G?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B00007EDM8?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B0042A8CW2?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B006ZH0K7A?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B00BBAFWNS?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B0072B9150?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B00001WRSJ?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B006R43YS8?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B006ZH0KJS?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B0002D03ZW?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B008GGH4FY?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B004RKVXXM?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B007T35O7U?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B008SYWFNA?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B008GFRE5A?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B006K5KRH0?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B006K5I9OI?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B006K5JC12?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B006K5I9OI?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B0000642RX?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B003UT2E8E?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B004SUO1R6?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B000Q6UZBM?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B007BPKHJ6?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B006R43YS8?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B00902SFC4?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B0058ZYNBE?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B005ARFEZO?condition=used&start");
		putter("www.amazon.com/gp/offer-listing/B0058ZYPL2?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B000Q6UZBM?condition=new&start");
		putter("www.amazon.com/gp/offer-listing/B005KDYBIO?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B004SUILFO?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B008OEHPKM?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B008GGH4FY?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B000UL5YCS?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B004V94F5C?condition=refurbished&start");
		putter("www.amazon.com/gp/offer-listing/B003RC92VG?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B00917EZPO?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B003BIGDN6?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B007I8FB7O?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B00BCGRVSU?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B0071O4ETQ?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/0972313303?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B0033A2X6I?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B004ZLV5AY?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B005SSB0YO?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B001YHI34S?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B000N021DQ?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B00BCGRXJC?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B004ZLV5UE?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B004V7E7M0?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B008HMT6EO?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B00BJKKTRO?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B0034L3G8U?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B007BY3PBO?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B000NUYW92?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B009A6CZYO?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B00BBAFYWW?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B004VHF210?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B004RE3YU8?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B00BBAFYWW?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B0074WVYWA?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B0012S4APK?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B0012S4APK?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B004RKVXXM?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B004OVDUKI?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B001EZYMF4?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B00470S8SK?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B00BBAFYWC?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B000OMZDJW?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B000LRVLQ4?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B0015AFONW?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B0058ZYNBE?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B008SYWFNA?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B008GFRE5A?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B0086824IE?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B0077CZJC8?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B006K5FB58?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B001S4OTS6?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B006K5FB58?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B008OEHPKM?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B004JO16KG?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B004V94F5C?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B006K5KRH0?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B003TFEQQC?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B000KNE14S?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B00018MSNI?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B006K5FB58?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B00470OX02?condition=new&startIn");
		putter("www.amazon.com/gp/offer-listing/B0058ZZSG8?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B00BAJWI6E?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B006K4MMZQ?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B0079UAT0A?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B004SUO1R6?condition=used&startIn");
		putter("www.amazon.com/gp/offer-listing/B006K4LZK4?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B0058ZZMZU?condition=refurbished&startIn");
		putter("www.amazon.com/gp/offer-listing/B004SUIMGW?condition=used&startIn");

	}

	private void putter(String str) {
		int x = str.indexOf("offer-listing");
		String asin = str.substring(x + 14, x + 24);

		Map<String, Object> data = new HashMap<String, Object>();
		data.put("asin", asin);

		JobOptions options = new JobOptions.OptionsBuilder().data(data).build();
		_queue.put("com.ciq.qless.jobs.AmazonOfferUrlGenerator", options, data);
	}

	@Test
	public void testQueueGetJobs() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();

		popJob();
		popJob();

		List<String> jids = _queue.jobs().running();
		assertTrue(jids.contains(jid1));
		assertTrue(jids.contains(jid2));

		removeJobs(jid1, jid2);
	}

	@Test
	public void testGetQueueName() {
		assertEquals("test-queue", _queue.name());
	}

	@Test
	public void testQueueGetCount() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();
		popJob();
		popJob();
		String jid3 = addDelayedJob(60);
		String jid4 = addDependentJob(Arrays.asList(jid1, jid2));
		String jid5 = addRecurringJob();
		String jid6 = addRecurringJob();

		Map<String, Object> counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(2, counts.get("running"));
		assertEquals(1, counts.get("scheduled"));
		assertEquals(1, counts.get("depends"));
		assertEquals(2, counts.get("recurring"));

		removeJobs(jid1, jid2, jid3, jid4, jid5, jid6);
	}

	@Test
	public void testQueuePause() throws LuaScriptException {
		String jid1 = addJob();

		_queue.pause();

		popJob();

		// Pop should not effect the job, since the queue is paused
		Map<String, Object> counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(0, counts.get("running"));

		removeJobs(jid1);
	}

	@Test
	public void testQueueUnpause() throws LuaScriptException {
		String jid1 = addJob();

		_queue.pause();

		popJob();

		// Pop should not effect the job, since the queue is paused
		Map<String, Object> counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(0, counts.get("running"));

		_queue.unpause();

		popJob();

		counts = _queue.count();
		assertEquals("test-queue", counts.get("name").toString());
		assertEquals(1, counts.get("running"));

		removeJobs(jid1);
	}

	@Test
	public void testQueueGetSetHeartbeat() {
		String hb = _queue.getHeartbeat();
		assertEquals("", hb);

		_queue.setHeartbeat(65);

		hb = _queue.getHeartbeat();
		assertEquals("65", hb);
	}

	@Test
	public void testQueuePutNewJob() throws LuaScriptException {
		JobOptions options = new JobOptions.OptionsBuilder(TEST_JID).build();

		Map<String, Object> data = new HashMap<String, Object>();
		data.put("url", "http://www.homepage.com");
		_queue.put(SimpleTestJob.class.getName(), options, data);

		BaseJob job = _queue.pop();

		assertEquals(TEST_JID, job.getAttributes().getJID());
		assertTrue(job.getAttributes().getData().containsKey("url"));
		assertTrue(job.getAttributes().getData().get("url")
				.equals("http://www.homepage.com"));

		removeJobs(TEST_JID);
	}

	@Test
	public void testQueuePutNewRecurringJob() throws Exception {
		JobOptions options = new JobOptions.OptionsBuilder(TEST_JID).build();

		Map<String, Object> data = new HashMap<String, Object>();
		data.put("url", "http://www.homepage.com");
		_queue.recur(SimpleTestJob.class.getName(), data, 1, options);

		BaseJob job = _queue.pop();
		Thread.sleep(2000);
		job.complete();

		Thread.sleep(1000);
		job = _queue.pop();

		assertTrue(job.getAttributes().getJID().startsWith(TEST_JID));
		assertTrue(job.getAttributes().getData().containsKey("url"));
		assertTrue(job.getAttributes().getData().get("url")
				.equals("http://www.homepage.com"));

		removeJobs(TEST_JID);
	}

	@Test
	public void testQueuePopSingleJob() throws LuaScriptException {
		String jid = addJob();

		BaseJob job = _queue.pop();

		assertEquals(jid, job.getAttributes().getJID());

		removeJobs(jid);
	}

	@Test
	public void testQueuePopMultipleJobs() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addJob();

		List<BaseJob> jobs = _queue.pop(2);

		assertEquals(2, jobs.size());

		for (BaseJob baseJob : jobs) {
			assertTrue(baseJob.getAttributes().getJID().equals(jid1)
					|| baseJob.getAttributes().getJID().equals(jid2));
		}

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueuePeekTopJob() throws LuaScriptException,
			InterruptedException {
		String jid1 = addJob();
		Thread.sleep(1000);
		String jid2 = addJob();

		BaseJob job = _queue.peek();

		assertEquals(jid1, job.getAttributes().getJID());

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueuePeekMultipleJobs() throws LuaScriptException,
			InterruptedException {
		String jid1 = addJob();
		Thread.sleep(1000);
		String jid2 = addJob();

		List<BaseJob> jobs = _queue.peek(2);

		assertEquals(2, jobs.size());

		for (BaseJob baseJob : jobs) {
			assertTrue(baseJob.getAttributes().getJID().equals(jid1)
					|| baseJob.getAttributes().getJID().equals(jid2));
		}

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueuePeekDoesNotRemoveJobs() throws LuaScriptException,
			InterruptedException {
		String jid1 = addJob();
		Thread.sleep(1000);
		String jid2 = addJob();

		BaseJob job = _queue.peek();

		assertEquals(jid1, job.getAttributes().getJID());

		job = _queue.peek();

		assertEquals(jid1, job.getAttributes().getJID());

		removeJobs(jid1, jid2);
	}

	@Test
	public void testQueueGetStats() throws LuaScriptException {
		String jid = addNewTestJob();
		String jid2 = addJob();

		// Simulate Work
		popJob();

		List<String> keys = new ArrayList<String>();
		List<String> args = Arrays.asList(TEST_QUEUE,
				JQlessClient.getCurrentSeconds());

		Map<String, Object> stats = _queue.stats();

		assertTrue(stats.containsKey("retries"));
		assertTrue(stats.containsKey("failed"));
		assertTrue(stats.containsKey("failures"));
		assertTrue(stats.containsKey("wait"));
		assertTrue(stats.containsKey("run"));

		removeJob(jid);
		removeJob(jid2);
	}

	@Test
	public void testQueueGetLength() throws LuaScriptException {
		String jid1 = addJob();
		String jid2 = addDelayedJob(60);

		popJob();
		popJob();

		int length = _queue.length();

		assertEquals(2, length);

		removeJobs(jid1, jid2);
	}
}
