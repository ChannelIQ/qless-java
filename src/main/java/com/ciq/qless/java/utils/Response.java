package com.ciq.qless.java.utils;


public abstract class Response<T> {
	public abstract T build(Object data);
}
