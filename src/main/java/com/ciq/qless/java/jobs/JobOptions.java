package com.ciq.qless.java.jobs;

import java.util.ArrayList;
import java.util.List;

public class JobOptions {
	private String _jid;
	private int _delay;
	private int _priority;
	private int _offset;
	private int _retries;
	private List<String> _depends;
	private List<String> _tags;

	private JobOptions(OptionsBuilder builder) {
		_jid = builder._jid;
		_delay = builder._delay;
		_priority = builder._priority;
		_offset = builder._offset;
		_retries = builder._retries;
		_depends = builder._depends;
		_tags = builder._tags;
	}

	public static class OptionsBuilder {
		// Required fields
		private final String _jid;

		// Optional fields
		private int _delay = 0;
		private int _priority = 0;
		private int _offset = 0;
		private int _retries = 5;
		private List<String> _depends = new ArrayList<String>();
		private List<String> _tags = new ArrayList<String>();

		public OptionsBuilder(String jid) {
			this._jid = jid;
		}

		public OptionsBuilder delay(int delay) {
			this._delay = delay;
			return this;
		}

		public OptionsBuilder priority(int priority) {
			this._priority = priority;
			return this;
		}

		public OptionsBuilder offset(int offset) {
			this._offset = offset;
			return this;
		}

		public OptionsBuilder retries(int retries) {
			this._retries = retries;
			return this;
		}

		public OptionsBuilder depends(String... depends) {
			ArrayList<String> deps = new ArrayList<String>();
			for (String depend : depends) {
				deps.add(depend);
			}
			this._depends = deps;
			return this;
		}

		public OptionsBuilder tags(String... tags) {
			ArrayList<String> t = new ArrayList<String>();
			for (String tag : tags) {
				t.add(tag);
			}
			this._tags = t;
			return this;
		}

		public JobOptions build() {
			return new JobOptions(this);
		}
	}

	public String getJID() {
		return this._jid;
	}

	public void setJID(String jid) {
		_jid = jid;
	}

	public List<String> getDepends() {
		return this._depends;
	}

	public void setDepends(List<String> depends) {
		_depends = depends;
	}

	public int getPriority() {
		return _priority;
	}

	public void setPriority(int priority) {
		_priority = priority;
	}

	public List<String> getTags() {
		return this._tags;
	}

	public void setTags(List<String> tags) {
		_tags = tags;
	}

	public int getDelay() {
		return _delay;
	}

	public void setDelay(int delay) {
		_delay = delay;
	}

	public int getOffset() {
		return _offset;
	}

	public void setOffset(int offset) {
		_offset = offset;
	}

	public int getRetries() {
		return _retries;
	}

	public void setRetries(int retries) {
		_retries = retries;
	}
}