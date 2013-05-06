package com.ciq.jqless.client;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class ClientEvents {
	public final Jedis _jedis;
	public final JedisPubSub _jedisPubSub;

	public ClientEvents(Jedis jedis) {
		this._jedis = jedis;

		this._jedisPubSub = new JedisPubSub() {

			@Override
			public void onUnsubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onSubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onPUnsubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onPSubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onPMessage(String arg0, String arg1, String arg2) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onMessage(String arg0, String arg1) {
				// TODO Auto-generated method stub

			}
		};
	}

	public void listen() {
		this._jedis.subscribe(this._jedisPubSub, ":canceled", ":completed",
				":failed", ":popped", ":stalled", ":put", ":track", ":untrack");
	}

	public void hangup() {
		this._jedisPubSub.unsubscribe();
	}
}
