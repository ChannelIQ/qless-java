package com.ciq.qless.java.lua;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class LuaScript {
	private final JedisPool _pool;
	private String _sha1Key = "";

	private static final Logger _logger = LoggerFactory
			.getLogger(LuaScript.class);

	public LuaScript(JedisPool pool) {
		this._pool = pool;
	}

	public Object callScript(String scriptName, List<String> keys,
			List<String> args) throws LuaScriptException {
		String scriptContent = getScript(scriptName);
		this._sha1Key = getScriptHash(scriptContent);

		try {
			Object o = callScriptByHash(_sha1Key, keys, args);
			return o;
		} catch (Exception e) {
			_logger.info("An attempt to call script (" + scriptName
					+ ") resulted in an exception - " + e.getMessage()
					+ ".  Reloading the script will be attempted.");
			this._sha1Key = reloadScript(scriptName);
			try {
				return callScriptByHash(_sha1Key, keys, args);
			} catch (Exception ex) {
				// Log
				_logger.error("A call to script (" + scriptName
						+ ") resulted in an exception - " + e.getMessage());
				throw new LuaScriptException(ex);
			}
		}
	}

	private String reloadScript(String scriptName) throws LuaScriptException {
		Jedis jedis = this._pool.getResource();
		try {
			return jedis.scriptLoad(getScript(scriptName));
		} catch (Exception ex) {
			_logger.error("Unable to load script (" + scriptName
					+ ") from Redis.  Exception - " + ex.getMessage());
			this._pool.returnBrokenResource(jedis);
			throw new LuaScriptException(ex);
		} finally {
			this._pool.returnResource(jedis);
		}
	}

	private Object callScriptByHash(String sha, List<String> keys,
			List<String> args) throws Exception {
		Jedis jedis = this._pool.getResource();
		try {
			return jedis.evalsha(sha, keys, args);
		} catch (Exception ex) {
			_logger.error("A call to script with SHA (" + sha
					+ ") resulted in an exception - " + ex.getMessage());
			this._pool.returnBrokenResource(jedis);
			throw new LuaScriptException(ex);
		} finally {
			this._pool.returnResource(jedis);
		}
	}

	public String getScript(String scriptName) {
		InputStream stream = null;
		try {
			stream = ClassLoader.getSystemResourceAsStream(scriptName);

			@SuppressWarnings("resource")
			Scanner s = new Scanner(stream).useDelimiter("\\A");

			return s.hasNext() ? s.next() : "";
		} catch (Exception e) {
			_logger.error("Failed to open the " + scriptName
					+ " for processing");
		} finally {
			if (stream != null)
				try {
					stream.close();
				} catch (IOException e) {
					_logger.error("Failed to close the script file Input Stream for file "
							+ scriptName);
				}
		}

		return "";
		// throw new Exception("Script file is missing: " + scriptName);
	}

	// Push all this somewhere else?
	public String getScriptHash(String script) {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			// Log it
			_logger.error("Unable to calculate script hash for (" + script
					+ ") resulted in an exception - " + e.getMessage());
		}
		return byteArrayToHexString(md.digest(script.getBytes()));
	}

	public static String byteArrayToHexString(byte[] b) {
		String result = "";
		for (int i = 0; i < b.length; i++) {
			result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
		}
		return result;
	}
}
