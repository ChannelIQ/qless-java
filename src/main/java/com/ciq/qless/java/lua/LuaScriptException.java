package com.ciq.qless.java.lua;

public class LuaScriptException extends Exception {
	private static final long serialVersionUID = 225571035410690001L;

	public LuaScriptException(Exception ex) {
		super(ex);
	}

	public String getMethod() {
		int key = super.getMessage().indexOf("user_script:");
		int start = super.getMessage().indexOf(": ", key);
		int msgStart = super.getMessage().indexOf("(): ", start);
		return super.getMessage().substring(start + 2, msgStart).trim();
	}

	@Override
	public String getMessage() {
		int key = super.getMessage().indexOf("user_script:");
		int start = super.getMessage().indexOf(": ", key);
		int msgStart = super.getMessage().indexOf("(): ", start);
		return super.getMessage().substring(msgStart + 4).trim();
	}

}
