package com.ciq.qless.java.test;

import java.lang.reflect.Constructor;
import java.util.List;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * <p>
 * This <code>GuiceTestRunner</code> class is used for running tests that have
 * dependencies that should be loaded by Guice.
 * </p>
 * <p>
 * The test class should also define <code>@WithModules</code> annotation.
 * </p>
 * 
 * @author jsymons
 * 
 */
public class GuiceTestRunner extends BlockJUnit4ClassRunner {
	private Injector injector;

	public GuiceTestRunner(Class<?> klass) throws InitializationError {
		super(klass);

		// Check the @WithModules annotation
		WithModules wm = this.getTestClass().getJavaClass()
				.getAnnotation(WithModules.class);

		if (wm == null) {
			throw new InitializationError(
					"The class should have the @WithModules annotation");
		} else {
			if (wm.value().length == 0) {
				throw new InitializationError(
						"The @WithModules annotation should not be empty.");
			}
		}
	}

	/**
	 * Create the Guice injector
	 * 
	 * @return New injector
	 */
	protected Injector createInjector() throws Exception {
		// Extract the modules classes
		WithModules wm = this.getTestClass().getJavaClass()
				.getAnnotation(WithModules.class);
		Class<? extends Module>[] moduleClasses = wm.value();

		// Instantiate the modules
		Module[] modules = new Module[moduleClasses.length];
		for (int i = 0; i < moduleClasses.length; i++) {
			if (moduleClasses[i] == null) {
				throw new Exception(
						"A module class within the @WithModules annotation cannot be null.");
			}
			try {
				modules[i] = moduleClasses[i].newInstance();
			} catch (Exception e) {
				throw new Exception(
						"The module "
								+ moduleClasses[i]
								+ " is required by the test and cannot be instantiated.",
						e);
			}
		}

		// Return the injector
		return Guice.createInjector(modules);
	}

	@Override
	protected Object createTest() throws Exception {
		return ensureInjector().getInstance(this.getTestClass().getJavaClass());
	}

	protected final Injector ensureInjector() throws Exception {
		if (this.injector == null) {
			this.injector = createInjector();
		}
		return this.injector;
	}

	@Override
	protected void validateConstructor(List<Throwable> errors) {
		// Get the constructor
		Constructor<?>[] constructors = this.getTestClass().getJavaClass()
				.getDeclaredConstructors();
		if (constructors.length == 0) {
			return;
		}

		// Search for a valid one
		for (Constructor<?> c : constructors) {
			// Modifiers: 0: package, 1: public, 2: private, 4: protected
			boolean hasAccess = (c.getModifiers() != 2);
			boolean noArgs = (c.getParameterTypes().length == 0);
			boolean hasInject = (c.getAnnotation(Inject.class) != null);
			if (hasAccess && (noArgs || hasInject)) {
				return;
			}
		}

		errors.add(new Exception(
				"The class should have a constructor with the @Inject annotation or one with no argument."));
	}

}
