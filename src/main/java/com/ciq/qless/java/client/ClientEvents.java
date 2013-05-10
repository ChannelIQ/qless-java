package com.ciq.qless.java.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class ClientEvents {
	public final Jedis _jedis;
	private ClientEventListener _clientEventListener;
	private final Map<String, ArrayList<ClientEventCallback>> _callbacks;
	private final ExecutorService _pool;

	public ClientEvents(Jedis jedis) {
		_jedis = jedis;
		_callbacks = new HashMap<String, ArrayList<ClientEventCallback>>();
		_pool = Executors.newFixedThreadPool(20);
	}

	public void registerCallback(ClientEventCallback callback,
			String... channels) {
		for (String channel : channels) {
			ArrayList<ClientEventCallback> callbacks = new ArrayList<ClientEventCallback>();

			if (_callbacks.containsKey(channel)) {
				callbacks = _callbacks.get(channel);
			}

			callbacks.add(callback);
			_callbacks.put(channel, callbacks);
		}
	}

	public void listen() {
		new Thread(new Runnable() {
			public void run() {
				Jedis subscriberJedis = new Jedis("localhost");
				try {
					_clientEventListener = new ClientEventListener();
					_jedis.subscribe(_clientEventListener, ":canceled",
							"completed", "failed", "popped", "stalled", "put",
							"track", "untrack");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();

	}

	public void hangup() {
		_clientEventListener.unsubscribe();
	}

	class ClientEventListener extends JedisPubSub {

		@Override
		public void onMessage(String channel, String message) {
			if (_callbacks.containsKey(channel)) {
				for (ClientEventCallback callback : _callbacks.get(channel)) {
					callback.setMessage(channel, message);
					_pool.execute(callback);
				}
			}
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
		}

		@Override
		public void onSubscribe(String channel, int subscribedChannels) {
		}

		@Override
		public void onUnsubscribe(String channel, int subscribedChannels) {
		}

		@Override
		public void onPUnsubscribe(String pattern, int subscribedChannels) {
		}

		@Override
		public void onPSubscribe(String pattern, int subscribedChannels) {
		}

	}

}
