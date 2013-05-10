package com.ciq.qless.java.utils;

import com.ciq.qless.java.client.JQlessClient;

public abstract class ComplexResponse<T> {
	public abstract T build(Object data, JQlessClient client);
}
