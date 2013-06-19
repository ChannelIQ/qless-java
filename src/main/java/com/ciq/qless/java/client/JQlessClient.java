package com.ciq.qless.java.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

import com.ciq.qless.java.lua.LuaScript;
import com.ciq.qless.java.lua.LuaScriptObject;
import com.ciq.qless.java.queues.Queue;
import com.ciq.qless.java.utils.ResponseFactory;

public class JQlessClient {
	private final JedisPool _jedisPool;
	private final Config _config;
	private final ClientJobs _clientJobs;
	private final ClientWorkers _clientWorkers;
	private final ClientQueues _clientQueues;
	private final ClientEvents _clientEvents;

	private final LuaScript _scriptRunner;

	private static final Logger _logger = LoggerFactory
			.getLogger(JQlessClient.class);

	public JQlessClient(JedisPool pool) {
		this._jedisPool = pool;
		// Options

		// Configure Self
		_config = new Config(this);

		// Jobs
		_clientJobs = new ClientJobs(this);

		// Queues
		_clientQueues = new ClientQueues(this);

		// Workers
		_clientWorkers = new ClientWorkers(this);

		// Client Events
		// TODO: REFACTOR THIS TO GET A NEW JEDIS INSTANCE ONLY FOR LISTENING TO
		// EVENTS
		_clientEvents = new ClientEvents(this._jedisPool);

		// Callbacks
		_scriptRunner = new LuaScript(this._jedisPool);
	}

	public ClientJobs Jobs() {
		return _clientJobs;
	}

	public ClientWorkers Workers() {
		return _clientWorkers;
	}

	public ClientQueues Queues() {
		return _clientQueues;
	}

	public Queue Queue(String name) {
		return _clientQueues.getNamedQueue(name);
	}

	public Config Config() {
		return _config;
	}

	public int getQueueLength(String name) {
		int count = 0;

		Jedis jedis = this._jedisPool.getResource();
		try {
			Transaction trans = jedis.multi();
			trans.zcard("ql:q:" + name + "-locks");
			trans.zcard("ql:q:" + name + "-work");
			trans.zcard("ql:q:" + name + "-scheduled");
			trans.zcount("ql:q:" + name + "-recur", 0,
					Integer.parseInt(getCurrentSeconds()));
			List<Object> allItems = trans.exec();

			for (Object obj : allItems) {
				count += Integer.parseInt(obj.toString());
			}
		} catch (JedisException je) {
			_logger.error("Exception getting QueueLength: " + je.getMessage());
		} catch (Exception e) {
			_logger.error("Exception getting QueueLength: " + e.getMessage());
			this._jedisPool.returnBrokenResource(jedis);
		} finally {
			this._jedisPool.returnResource(jedis);
		}

		return count;
	}

	public String getConfig(String key) {
		return this._config.get(key);
	}

	public void setConfig(String string, Object value) {
		this._config.set(string, value);
	}

	public boolean track(String jid) {
		List<String> params = Arrays.asList("track", jid, getCurrentSeconds());

		return this.call(Command.TRACK, params).as(ResponseFactory.BOOLEAN);
	}

	public boolean untrack(String jid) {
		List<String> params = Arrays
				.asList("untrack", jid, getCurrentSeconds());

		return this.call(Command.TRACK, params).as(ResponseFactory.BOOLEAN);
	}

	public List<String> tags() {
		return tags(0, 100);
	}

	public List<String> tags(int offset, int count) {
		List<String> params = Arrays.asList("top", String.valueOf(offset),
				String.valueOf(count));

		return this.call(Command.TAG, params).as(ResponseFactory.TAGS);
	}

	public void deRegisterWorkers(String... workerNames) {
		ArrayList<String> args = new ArrayList<String>();
		for (String worker : workerNames) {
			args.add(worker);
		}
		this.call(Command.DEREGISTER_WORKERS, args);
	}

	public void cancel(String... jids) {
		ArrayList<String> args = new ArrayList<String>();
		for (String jid : jids) {
			args.add(jid);
		}

		this.call(Command.CANCEL, args);
	}

	public static String getMachineName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			_logger.error("Unable to determine machine name: " + e.getMessage());
			return "unknown-host";
		}
	}

	public static String getCurrentSeconds() {
		return String.valueOf(new DateTime(System.currentTimeMillis(),
				DateTimeZone.UTC).getMillis() / 1000);
	}

	public void close() {
		_jedisPool.destroy();
	}

	// EVENTS SECTION - this will require another instance of Jedis running in
	// the background to fully support eventing

	public static enum Command {
		CANCEL, CONFIG, COMPLETE, DEPENDS, FAIL, FAILED, GET, HEARTBEAT, JOBS, PEEK, POP, PRIORITY, PUT, QUEUES, RECUR, RETRY, STATS, TAG, TRACK, WORKERS, PAUSE, UNPAUSE, DEREGISTER_WORKERS;

		Command() {
		}

		public String scriptName() {
			return this.toString().toLowerCase() + ".lua";
		}
	}

	public LuaScriptObject call(Command command, List<String> args) {
		return this.call(command, new ArrayList<String>(), args);
	}

	public LuaScriptObject call(Command command, List<String> keys,
			List<String> args) {
		try {
			Object o = this._scriptRunner.callScript(command.scriptName(),
					keys, args);
			return new LuaScriptObject(o);
		} catch (Exception ex) {
			_logger.error("JQlessClient Problem calling scripts for command ("
					+ command.scriptName() + ") : " + ex.getMessage());
			return new LuaScriptObject(new Object());
		}
	}
}
