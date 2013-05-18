package com.ciq.qless.java.test.client;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

import com.ciq.qless.java.client.Config;
import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.test.BaseTest;

public class ConfigTest extends BaseTest {

	private static Config _config;

	@BeforeClass
	public static void init() {
		JedisPool pool = new JedisPool("localhost");
		// Jedis jedis = new Jedis("localhost");
		_config = new Config(new JQlessClient(pool));
	}

	@Test
	public void testGetConfigDefaults() {
		String configValue = _config.get(Config.APPLICATION);
		assertEquals(Config.APPLICATION_DEFAULT, configValue);

		configValue = _config.get(Config.HEARTBEAT);
		assertEquals(Config.HEARTBEAT_DEFAULT, configValue);

		configValue = _config.get(Config.STATS_HISTORY);
		assertEquals(Config.STATS_HISTORY_DEFAULT, configValue);

		configValue = _config.get(Config.HISTOGRAM_HISTORY);
		assertEquals(Config.HISTOGRAM_HISTORY_DEFAULT, configValue);

		configValue = _config.get(Config.JOB_HISTORY_COUNT);
		assertEquals(Config.JOB_HISTORY_COUNT_DEFAULT, configValue);

		configValue = _config.get(Config.JOBS_HISTORY);
		assertEquals(Config.JOBS_HISTORY_DEFAULT, configValue);
	}

	@Test
	public void testSetAndGetConfig() {
		String newField = "test-config";
		String newValue = "100";

		_config.set(newField, newValue);

		String underTest = _config.get(newField);
		assertEquals(newValue, underTest);
	}

	@Test
	public void testGetAllConfigs() {
		_config.set("test-config", "100");
		Map<String, Object> all = _config.all();

		assertEquals(Config.HEARTBEAT_DEFAULT, all.get(Config.HEARTBEAT)
				.toString());
		assertEquals(Config.JOB_HISTORY_COUNT_DEFAULT,
				all.get(Config.JOB_HISTORY_COUNT).toString());
		assertEquals("100", all.get("test-config").toString());
	}

	@Test
	public void testClearConfig() {
		_config.set("test-config", "100");
		_config.set(Config.HEARTBEAT, "15");

		String hb = _config.get(Config.HEARTBEAT);
		assertEquals("15", hb);
		String testValue = _config.get("test-config");
		assertEquals("100", testValue);

		_config.clear(Config.HEARTBEAT);
		_config.clear("test-config");

		hb = _config.get(Config.HEARTBEAT);
		assertEquals("60", hb);
		testValue = _config.get("test-config");
		assertEquals("", testValue);

	}

}
