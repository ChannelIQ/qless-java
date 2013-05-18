package com.ciq.qless.java.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ciq.qless.java.client.JQlessClient;
import com.ciq.qless.java.jobs.Attributes;
import com.ciq.qless.java.jobs.BaseJob;

public class ResponseFactory {
	private static final Response<String> STRING = new Response<String>() {
		@Override
		public String build(Object data) {
			if (data instanceof String) {
				return (String) data;
			} else {
				return String.valueOf(data);
			}
		}

		@Override
		public String toString() {
			return "String";
		}
	};
	public static final Response<String> JSON = STRING;
	public static final Response<String> JID = STRING;
	public static final Response<String> STATUS = STRING;
	public static final Response<String> CONFIG = STRING;

	public static final Response<Boolean> BOOLEAN = new Response<Boolean>() {
		@Override
		public Boolean build(Object data) {
			return String.valueOf(data).equals("") ? false : true;
		}

		@Override
		public String toString() {
			return "Boolean";
		}
	};

	private static final Response<Long> LONG = new Response<Long>() {
		@Override
		public Long build(Object data) {
			if (data instanceof String) {
				return 0l;
			}
			return (Long) data;
		}

		@Override
		public String toString() {
			return "Integer";
		}
	};
	public static final Response<Long> HEARTBEAT = LONG;
	public static final Response<Long> RETRIES = LONG;

	private static final Response<List<String>> LIST = new Response<List<String>>() {
		@SuppressWarnings("unchecked")
		@Override
		public List<String> build(Object data) {
			if (data instanceof String) {
				String json = (String) data;
				json = JsonHelper.fixArrayField(json, "jobs", "stalled");
				List<String> list = JsonHelper.parseList(json);
				return list;
			} else {
				return (List<String>) data;
			}
		}

		@Override
		public String toString() {
			return "List<String>";
		};
	};
	public static final Response<List<String>> JIDS = LIST;
	public static final Response<List<String>> TAGS = LIST;

	public static final Response<List<String>> TAGGEDJIDS = new Response<List<String>>() {
		@Override
		public List<String> build(Object data) {
			String json = (String) data;
			json = JsonHelper.fixArrayField(json, "expired", "jobs");
			Map<String, Object> tracked = JsonHelper.parseMap(json);
			return (List<String>) tracked.get("jobs");
		}

		@Override
		public String toString() {
			return "List<String>";
		}
	};

	private static final Response<Map<String, Object>> MAP = new Response<Map<String, Object>>() {
		@Override
		public Map<String, Object> build(Object data) {
			String json = (String) data;
			json = JsonHelper.fixArrayField(json, "jobs", "stalled");
			Map<String, Object> worker = JsonHelper.parseMap(json);
			return worker;
		}

		@Override
		public String toString() {
			return "Map<String, Object>";
		};

	};
	public static final Response<Map<String, Object>> WORKER = MAP;
	public static final Response<Map<String, Object>> CONFIGS = MAP;
	public static final Response<Map<String, Object>> STATS = MAP;
	public static final Response<Map<String, Object>> QUEUE = MAP;
	public static final Response<Map<String, Object>> FAILS = MAP;

	private static final Response<List<Map<String, Object>>> LIST_OF_MAPS = new Response<List<Map<String, Object>>>() {
		@Override
		public List<Map<String, Object>> build(Object data) {
			if (data instanceof String) {
				String json = (String) data;
				json = JsonHelper.fixArrayField(json, "jobs", "stalled");
				List<Map<String, Object>> workers = JsonHelper.parseList(json);
				return workers;
			} else if (data instanceof List<?>) {
				List<String> json = (List<String>) data;
				List<? extends Map> queues = JsonHelper.parseList(json,
						List.class, Map.class);

				return (List<Map<String, Object>>) queues;
			} else {
				throw new IllegalArgumentException("Unknown type");
			}
		}

		@Override
		public String toString() {
			return "List<Map<String, Object>>";
		}
	};
	public static final Response<List<Map<String, Object>>> WORKERS = LIST_OF_MAPS;
	public static final Response<List<Map<String, Object>>> QUEUES = LIST_OF_MAPS;

	public static final ComplexResponse<BaseJob> JOB = new ComplexResponse<BaseJob>() {
		@Override
		public BaseJob build(Object data, JQlessClient client) {
			if (data instanceof String) {
				String json = (String) data;
				json = JsonHelper.fixArrayField(json, "dependents",
						"dependencies");

				if (json.equals("")) {
					return null;
				}

				Map<String, Object> jobMap = JsonHelper.parseMap(json);

				ClassLoader classLoader = ResponseFactory.class
						.getClassLoader();

				BaseJob job = createJob(classLoader, jobMap, client);

				return job;
			} else if (data instanceof List<?>) {
				List<String> json = (List<String>) data;
				List<? extends Map> jobs = JsonHelper.parseList(json,
						List.class, Map.class);

				ClassLoader classLoader = ResponseFactory.class
						.getClassLoader();

				for (Map<String, Object> map : jobs) {
					BaseJob job = createJob(classLoader, map, client);

					if (job != null)
						return job;
				}

				return null;
			} else {
				throw new IllegalArgumentException("Unknown type for data");
			}
		};
	};
	public static final ComplexResponse<BaseJob> RECURRINGJOB = JOB;

	public static final ComplexResponse<List<BaseJob>> JOBS = new ComplexResponse<List<BaseJob>>() {
		@SuppressWarnings("unchecked")
		@Override
		public List<BaseJob> build(Object data, JQlessClient client) {
			try {
				if (data instanceof List<?>) {
					List<String> json = (List<String>) data;
					// json = JsonHelper.fixArrayField(json, "dependents",
					// "dependencies");
					List<Map> jobs = JsonHelper.parseList(json, List.class,
							Map.class);
					ArrayList<BaseJob> returnJobs = new ArrayList<BaseJob>();

					ClassLoader classLoader = ResponseFactory.class
							.getClassLoader();

					for (Map<String, Object> map : jobs) {
						BaseJob job = createJob(classLoader, map, client);
						returnJobs.add(job);
					}
					return returnJobs;
				} else {
					throw new IllegalArgumentException(
							"Unknown type for data - " + data.toString() + " "
									+ data.getClass().getName());
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return new ArrayList<BaseJob>();
		}

		@Override
		public String toString() {
			return "List<Job>";
		}
	};

	private static final ComplexResponse<List<BaseJob>> GROUPEDJOBS = new ComplexResponse<List<BaseJob>>() {
		@SuppressWarnings("unchecked")
		@Override
		public List<BaseJob> build(Object data, JQlessClient client) {
			String json = (String) data;
			json = JsonHelper.fixArrayField(json, "expired", "jobs",
					"dependents", "dependencies");
			Map<String, Object> tracked = JsonHelper.parseMap(json);
			List<Map<String, Object>> jobs = (List<Map<String, Object>>) tracked
					.get("jobs");
			ArrayList<BaseJob> returnJobs = new ArrayList<BaseJob>();

			ClassLoader classLoader = ResponseFactory.class.getClassLoader();

			for (Map<String, Object> map : jobs) {
				BaseJob job = createJob(classLoader, map, client);
				returnJobs.add(job);
			}

			return returnJobs;
		}

		@Override
		public String toString() {
			return "List<Job>";
		}
	};
	public static final ComplexResponse<List<BaseJob>> FAILEDJOBS = GROUPEDJOBS;
	public static final ComplexResponse<List<BaseJob>> TRACKEDJOBS = GROUPEDJOBS;

	private static BaseJob createJob(ClassLoader classLoader,
			Map<String, Object> map, JQlessClient client) {
		try {
			Attributes attrs = new Attributes(map);
			Class<?> klazz = classLoader.loadClass(attrs.getKlassName());

			@SuppressWarnings("rawtypes")
			Constructor c = klazz.getConstructor(JQlessClient.class,
					Attributes.class);
			if (c == null)
				c = klazz.getConstructor(JQlessClient.class);

			BaseJob job = (BaseJob) c.newInstance(client, attrs);
			System.out.println("aClass.getName() = " + klazz.getName());

			return job;
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (NoSuchMethodException e) {
			System.out.println(e.getMessage());
		} catch (SecurityException e) {
			System.out.println(e.getMessage());
		} catch (InstantiationException e) {
			System.out.println(e.getMessage());
		} catch (IllegalAccessException e) {
			System.out.println(e.getMessage());
		} catch (IllegalArgumentException e) {
			System.out.println(e.getMessage());
		} catch (InvocationTargetException e) {
			System.out.println(e.getMessage());
		}

		return null;
	}
}
