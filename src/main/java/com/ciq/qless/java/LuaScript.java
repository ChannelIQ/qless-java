package com.ciq.qless.java;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Scanner;

import redis.clients.jedis.Jedis;

public class LuaScript {
	private final Jedis _client;
	private String _sha1Key = "";

	public LuaScript(Jedis client) {
		this._client = client;
	}

	public Object callScript(String scriptName, List<String> keys,
			List<String> args) throws LuaScriptException {
		String scriptContent = getScript(scriptName);
		this._sha1Key = getScriptHash(scriptContent);

		try {
			Object o = callScriptByHash(_sha1Key, keys, args);
			return o;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			this._sha1Key = reloadScript(scriptName);
			try {
				return callScriptByHash(_sha1Key, keys, args);
			} catch (Exception ex) {
				// Log
				System.out.println(ex.getMessage());
				throw new LuaScriptException(ex);
			}
		}
	}

	private String reloadScript(String scriptName) {
		return _client.scriptLoad(getScript(scriptName));
	}

	private Object callScriptByHash(String sha, List<String> keys,
			List<String> args) {
		return _client.evalsha(sha, keys, args);
	}

	public String getScript(String scriptName) {
		InputStream stream = null;
		try {
			stream = ClassLoader.getSystemResourceAsStream(scriptName);

			@SuppressWarnings("resource")
			Scanner s = new Scanner(stream).useDelimiter("\\A");

			return s.hasNext() ? s.next() : "";
		} catch (Exception e) {
			System.out.println("Failed to open the " + scriptName
					+ " for processing");
		} finally {
			if (stream != null)
				try {
					stream.close();
				} catch (IOException e) {
					System.out
							.println("Failed to close the script file Input Stream for file "
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
