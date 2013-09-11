package com.ciq.qless.java;

import com.ciq.qless.java.utils.ResponseFactory;
import com.google.inject.PrivateModule;

public class QlessJavaModule extends PrivateModule {

	@Override
	protected void configure() {
		requestStaticInjection(ResponseFactory.class);
	}
}
