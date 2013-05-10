package com.ciq.qless.java.test.client;

import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.ciq.qless.java.client.ClientEventCallback;
import com.ciq.qless.java.client.ClientEvents;
import com.ciq.qless.java.lua.LuaScriptException;
import com.ciq.qless.java.test.BaseTest;

public class ClientEventsTest extends BaseTest {
	private static ClientEvents _clientEvents;

	@BeforeClass
	public static void init() {
		Jedis jedis = new Jedis("localhost");
		_clientEvents = new ClientEvents(jedis);
		_clientEvents.listen();
	}

	@Test
	public void testEventOnPop() throws LuaScriptException,
			InterruptedException {
		_clientEvents.registerCallback(new MyClientEventCallback(), "track",
				"popped");

		String jid1 = addJob();

		trackJob(jid1);

		popJob();

		System.out.println("test");
	}

	class MyClientEventCallback extends ClientEventCallback {
		private String _channel;
		private String _message;

		@Override
		public void setMessage(String channel, String message) {
			_channel = channel;
			_message = message;
		}

		@Override
		public void run() {
			System.out.println(_channel + ": " + _message);
		}
	}
}
