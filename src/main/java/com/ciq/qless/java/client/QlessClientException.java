package com.ciq.qless.java.client;

public class QlessClientException extends Exception {
	private static final long serialVersionUID = 8299657802767551934L;

	public QlessClientException(Exception ex) {
		super(ex);
	}

	public QlessClientException(String string) {
		super(string);
	}
}
