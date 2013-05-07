package com.ciq.qless.java.jobs;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ciq.qless.java.Queue;
import com.ciq.qless.java.client.JQlessClient;

public abstract class BaseJob {
	protected final JQlessClient _client;
	protected Attributes _attributes;

	private Queue _queue = null;

	public BaseJob(JQlessClient client, Attributes atts) {
		this._client = client;
	};

	public JQlessClient getClient() {
		return _client;
	}

	public Attributes getAttributes() {
		return _attributes;
	}

	public Queue getQueue() {
		if (_queue == null)
			_queue = new Queue(_attributes.getQueueName(), _client,
					JQlessClient.getMachineName());

		return _queue;
	}

	public void setPriority(int priority) {
		List<String> args = Arrays.asList("update", _attributes.getJID()
				.toString(), "priority", String.valueOf(priority));

		String retVal = (String) this._client.call(JQlessClient.Command.RECUR,
				args);

		_attributes.setPriority(priority);
	}

	public abstract void performWork();

	public Object getDataField(String field) {
		return _attributes.getData().get(field);
	}

	public void setDataField(String field, Object value) {
		Map<String, Object> data = _attributes.getData();

		data.put(field, value);

		_attributes.setData(data);
	}

}
