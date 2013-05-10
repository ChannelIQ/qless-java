package com.ciq.qless.java.client;

public abstract class ClientEventCallback implements Runnable {
	public abstract void setMessage(String channel, String message);

	public abstract void run();
}
