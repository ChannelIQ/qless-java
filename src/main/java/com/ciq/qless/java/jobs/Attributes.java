package com.ciq.qless.java.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

public class Attributes {
	private final Map<String, Object> _attributes;

	// private UUID _jid;
	// private Map<String, Object> _data;
	// private String _klassName;
	// private int _priority;
	// private List<String> _tags;
	// private String _workerName;
	// private LocalDateTime _expiresAt;
	// private String _state;
	// private boolean _tracked;
	// private String _queueName;
	// private int _retries;
	// private int _retriesRemaining;
	// private Map<String, Object> _failure;
	// private Map<String, Object> _history;
	// private List<Object> _dependencies;
	// private List<Object> _dependents;

	public Attributes() {
		_attributes = new HashMap<String, Object>();

		setDefaults();
	}

	public Attributes(Map<String, Object> attributes) {
		this();

		_attributes.putAll(attributes);
	}

	private void setDefaults() {
		setJID(UUID.randomUUID());
		setData(new HashMap<String, Object>());
		setKlassName("");
		setPriority(0);
		setTags(new ArrayList<String>());
		setWorkerName("mock_worker");
		setExpiresAt(LocalDateTime.now().plusHours(1)); // Default to 1 hour
		setState("running");
		setTracked(false);
		setQueueName("mock_queue");
		setRetries(5);
		setInterval(60);
		setRetriesRemaining(5);
		setFailure(new HashMap<String, Object>());
		setHistory(new ArrayList<HashMap<String, Object>>());
		setDependencies(new ArrayList<String>());
		setDependents(new ArrayList<String>());
	}

	public Map<String, Object> getHash() {
		return _attributes;
	}

	public String getJID() {
		return _attributes.get("jid").toString();
	}

	public void setJID(UUID jid) {
		_attributes.put("jid", jid.toString());
	}

	public void setJID(String jid) {
		_attributes.put("jid", jid);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getData() {
		return (Map<String, Object>) _attributes.get("data");
	}

	public void setData(Map<String, Object> data) {
		_attributes.put("data", data);
	}

	public String getKlassName() {
		return (String) _attributes.get("klass");
	}

	public void setKlassName(String klassName) {
		_attributes.put("klass", klassName);
	}

	public int getPriority() {
		return (Integer) _attributes.get("priority");
	}

	public void setPriority(int priority) {
		_attributes.put("priority", priority);
	}

	@SuppressWarnings("unchecked")
	public List<String> getTags() {
		return (List<String>) _attributes.get("tags");
	}

	public void setTags(List<String> tags) {
		_attributes.put("tags", tags);
	}

	public String getWorkerName() {
		return (String) _attributes.get("worker");
	}

	public void setWorkerName(String workerName) {
		_attributes.put("worker", workerName);
	}

	public LocalDateTime getExpiresAt() {
		return new LocalDateTime(Long.valueOf(_attributes.get("expires")
				.toString()), DateTimeZone.UTC);
	}

	public void setExpiresAt(LocalDateTime expiresAt) {
		_attributes.put("expires", expiresAt);
	}

	public String getState() {
		return (String) _attributes.get("state");
	}

	public void setState(String state) {
		_attributes.put("state", state);
	}

	public boolean isTracked() {
		return (Boolean) _attributes.get("tracked");
	}

	public void setTracked(boolean tracked) {
		_attributes.put("tracked", tracked);
	}

	public String getQueueName() {
		return (String) _attributes.get("queue");
	}

	public void setQueueName(String queueName) {
		_attributes.put("queue", queueName);
	}

	public int getRetries() {
		return (Integer) _attributes.get("retries");
	}

	public void setRetries(int retries) {
		_attributes.put("retries", retries);
	}

	public int getInterval() {
		return (Integer) _attributes.get("interval");
	}

	public void setInterval(int interval) {
		_attributes.put("interval", interval);
	}

	public int getRetriesRemaining() {
		return (Integer) _attributes.get("remaining");
	}

	public void setRetriesRemaining(int retriesRemaining) {
		_attributes.put("remaining", retriesRemaining);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getFailure() {
		return (Map<String, Object>) _attributes.get("failure");
	}

	public void setFailure(Map<String, Object> failure) {
		_attributes.put("failure", failure);
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> getHistory() {
		return (List<Map<String, Object>>) _attributes.get("history");
	}

	public void setHistory(List<HashMap<String, Object>> history) {
		_attributes.put("history", history);
	}

	@SuppressWarnings("unchecked")
	public List<String> getDependencies() {
		return (List<String>) _attributes.get("dependencies");
	}

	public void setDependencies(List<String> dependencies) {
		_attributes.put("dependencies", dependencies);
	}

	@SuppressWarnings("unchecked")
	public List<String> getDependents() {
		return (List<String>) _attributes.get("dependents");
	}

	public void setDependents(List<String> dependents) {
		_attributes.put("dependents", dependents);
	}

}
