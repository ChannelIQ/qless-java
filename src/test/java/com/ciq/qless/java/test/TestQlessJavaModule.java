package com.ciq.qless.java.test;

import com.ciq.qless.java.QlessJavaModule;
import com.ciq.qless.java.test.jobs.SimpleTestJob;
import com.ciq.qless.java.utils.GenericJobFactory;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class TestQlessJavaModule extends QlessJavaModule {

	@Override
	protected void configure() {
		super.configure();

		install(new FactoryModuleBuilder()
				.build(new TypeLiteral<GenericJobFactory<SimpleTestJob>>() {
				}));
	}

}
