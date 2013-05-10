package com.ciq.qless.java.lua;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.utils.ComplexResponse;
import com.ciq.qless.java.utils.Response;

public class LuaScriptObject {
	private final Object _object;

	public LuaScriptObject(Object o) {
		_object = o;
	}

	public <T> T as(Response<T> response) {
		return response.build(this.getObject());
	}

	public <T> T as(ComplexResponse<T> response, JQlessClient client) {
		return response.build(this.getObject(), client);
	}

	public Object getObject() {
		return _object;
	}
}
