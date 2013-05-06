package com.ciq.jqless.jobs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import redis.clients.util.SafeEncoder;

import com.ciq.jqless.Builder;

public class BuilderFactory {
	public static final Builder<Map<String, Object>> STRING_MAP = new Builder<Map<String, Object>>() {
		@Override
		@SuppressWarnings("unchecked")
		public Map<String, Object> build(Object data) {
			final List<byte[]> flatHash = (List<byte[]>) data;
			final Map<String, Object> hash = new HashMap<String, Object>();
			final Iterator<byte[]> iterator = flatHash.iterator();
			while (iterator.hasNext()) {
				hash.put(SafeEncoder.encode(iterator.next()), iterator.next());
			}

			return hash;
		}

		@Override
		public String toString() {
			return "Map<String, Object>";
		}

	};
}
