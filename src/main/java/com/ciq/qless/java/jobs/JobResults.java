package com.ciq.qless.java.jobs;

import java.util.ArrayList;
import java.util.List;

public class JobResults {
	private boolean _isSuccessful = false;
	private List<String> _warnings;
	private List<String> _errors;

	public JobResults() {
	}

	// Builder methods
	public static JobResults success() {
		JobResults results = new JobResults();
		results.setIsSuccessful(true);
		return results;
	}

	public static JobResults fail() {
		JobResults results = new JobResults();
		results.setIsSuccessful(false);
		return results;
	}

	public JobResults addError(String msg) {
		if (this._errors == null) {
			this._errors = new ArrayList<String>();
		}
		this._errors.add(msg);
		return this;
	}

	public JobResults addWarning(String msg) {
		if (this._warnings == null) {
			this._warnings = new ArrayList<String>();
		}
		this._warnings.add(msg);
		return this;
	}

	// Status Methods
	public boolean isSuccessful() {
		return _isSuccessful;
	}

	public void setIsSuccessful(boolean isSuccessful) {
		this._isSuccessful = isSuccessful;
	}

	public boolean hasErrors() {
		return (this._errors != null);
	}

	public List<String> getErrors() {
		if (this._errors == null) {
			this._errors = new ArrayList<String>();
		}
		return _errors;
	}

	public boolean hasWarnings() {
		return (this._warnings != null);
	}

	public List<String> getWarnings() {
		if (this._warnings == null) {
			this._warnings = new ArrayList<String>();
		}
		return _warnings;
	}
}
