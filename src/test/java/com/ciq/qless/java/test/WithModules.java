package com.ciq.qless.java.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.Module;

@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface WithModules {
	/**
	 * Array of all the modules' classes to load for configuring the Guice
	 * injector
	 * 
	 * @returns Modules' classes to load
	 * 
	 */
	public Class<? extends Module>[] value();
}
