package com.ciq.jqless.client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.joda.time.LocalDateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

import com.ciq.jqless.Config;
import com.ciq.jqless.LuaScript;

/*
 * Consider passing in a JedisPool instead of a JedisClient, that way we could spin up/down the instances
 * as needed by the JQlessClient.  May need refactoring later
 */
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

	public ClientJobs getJobs() {
		return _clientJobs;
	}

	public ClientWorkers getWorkers() {
		return _clientWorkers;
	}

	public ClientQueues getQueues() {
		return _clientQueues;
	}

	public int getQueueLength(String name) {
		int count = 0;
		try {
			Transaction trans = this._jedis.multi();
			this._jedis.zcard("q1:q:" + name + "-locks");
			this._jedis.zcard("q1:q:" + name + "-work");
			this._jedis.zcard("q1:q:" + name + "-scheduled");
			List<Object> allItems = trans.exec();

			for (Object obj : allItems) {
				count += Integer.parseInt(obj.toString());
			}
		} catch (JedisException je) {
			// log this shit
		}

		return count;
	}

	public String getConfig(String key) {
		return this._config.get(key);
	}

	public void setConfig(String string, Object value) {
		this._config.set(string, value);
	}

	public void track(UUID jid) {
		List<String> params = Arrays.asList("track", jid.toString(),
				getCurrentSeconds());

		this.call(Command.TRACK, params);
	}

	public void untrack(UUID jid) {
		List<String> params = Arrays.asList("untrack", jid.toString(),
				getCurrentSeconds());

		this.call(Command.TRACK, params);
	}

	public String tags() {
		return tags(0, 100);
	}

	public String tags(int offset, int count) {
		List<String> params = Arrays.asList("top", String.valueOf(offset),
				String.valueOf(count));

		// TODO: Return via JSON
		this.call(Command.TAG, params);

		return "";
	}

	public Object deRegisterWorkers(List<String> workerNames) {
		return this.call(Command.DEREGISTER_WORKER, workerNames);
	}

	public Object cancel(List<String> jids) {
		return this.call(Command.CANCEL, jids);
	}

	// EVENTS SECTION - this will require another instance of Jedis running in
	// the background to fully support eventing

	public static enum Command {
		CANCEL, CONFIG, COMPLETE, DEPENDS, FAIL, FAILED, GET, HEARTBEAT, JOBS, PEEK, POP, PRIORITY, PUT, QUEUES, RECUR, RETRY, STATS, TAG, TRACK, WORKERS, PAUSE, UNPAUSE, DEREGISTER_WORKER;

		Command() {
		}

		public String scriptName() {
			return this.toString().toLowerCase() + ".lua";
		}
	}

	public Object call(Command command, List<String> args) {
		return this.call(command, new ArrayList<String>(), args);
	}

	public Object call(Command command, List<String> keys, List<String> args) {
		try {
			return this._scriptRunner.callScript(command.scriptName(), keys,
					args);
		} catch (Exception ex) {
			System.out.println("Problem: " + ex.getMessage());
			return "";
		}
	}

	public static String getMachineName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return "unknown-host";
		}
	}

	public static String getCurrentSeconds() {
		return String
				.valueOf(LocalDateTime.now().toDateTime().getMillis() / 1000);
	}
}
