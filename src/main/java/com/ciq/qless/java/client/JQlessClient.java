package com.ciq.qless.java.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

import com.ciq.qless.java.lua.LuaScript;
import com.ciq.qless.java.lua.LuaScriptObject;
import com.ciq.qless.java.utils.ResponseFactory;

public class JQlessClient {
	private final Jedis _jedis;
	private final Config _config;
	private final ClientJobs _clientJobs;
	private final ClientWorkers _clientWorkers;
	private final ClientQueues _clientQueues;
	private final ClientEvents _clientEvents;

	private final LuaScript _scriptRunner;

	public JQlessClient(Jedis jedis) {
		this._jedis = jedis;
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
		_clientEvents = new ClientEvents(this._jedis);

		// Callbacks
		_scriptRunner = new LuaScript(this._jedis);
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

	public Config Config() {
		return _config;
	}

	public int getQueueLength(String name) {
		int count = 0;
		try {
			Transaction trans = this._jedis.multi();
			trans.zcard("ql:q:" + name + "-locks");
			trans.zcard("ql:q:" + name + "-work");
			trans.zcard("ql:q:" + name + "-scheduled");
			List<Object> allItems = trans.exec();

			for (Object obj : allItems) {
				count += Integer.parseInt(obj.toString());
			}
		} catch (JedisException je) {
			System.out.println(je.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
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
			return "unknown-host";
		}
	}

	public static String getCurrentSeconds() {
		return String.valueOf(LocalDateTime.now(DateTimeZone.UTC).toDateTime()
				.getMillis() / 1000);
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
			System.out.println("Problem: " + ex.getMessage());
			return new LuaScriptObject(new Object());
		}
	}
}
