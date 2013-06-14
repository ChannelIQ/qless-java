package com.ciq.qless.java.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonHelper {
	private static final Logger _logger = LoggerFactory
			.getLogger(JsonHelper.class);

	@SuppressWarnings("unchecked")
	public static <E> List<E> parseList(String json) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		List<E> result = null;
		try {
			jp = factory.createParser(json);
			result = jp.readValueAs(List.class);
		} catch (JsonParseException e) {
			_logger.error("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			_logger.error("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			_logger.error("IOException: " + e.getMessage());
		}

		return result;
	}

	public static <E, T> List<T> parseList(List<String> input, Class<E> clazz,
			Class<T> innerClazz) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		List<T> result = new ArrayList<T>();
		try {
			for (String json : input) {
				jp = factory.createParser(json);
				result.add(jp.readValueAs(innerClazz));
			}
		} catch (JsonParseException e) {
			_logger.error("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			_logger.error("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			_logger.error("IOException: " + e.getMessage());
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> parseMap(String json) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		Map<String, Object> result = null;
		try {
			jp = factory.createParser(json);
			result = jp.readValueAs(HashMap.class);
		} catch (JsonParseException e) {
			_logger.error("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			_logger.error("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			_logger.error("IOException: " + e.getMessage());
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> parseMapFirstObject(List<String> strings) {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = mapper.getFactory();
		JsonParser jp = null;
		Map<String, Object> result = null;
		try {
			jp = factory.createParser(strings.get(0));
			result = jp.readValueAs(HashMap.class);
		} catch (JsonParseException e) {
			_logger.error("JsonParseException: " + e.getMessage());
		} catch (JsonProcessingException e) {
			_logger.error("JsonProcessingException: " + e.getMessage());
		} catch (IOException e) {
			_logger.error("IOException: " + e.getMessage());
		}

		return result;
	}

	public static <T> String createJSON(T values) {
		final OutputStream out = new ByteArrayOutputStream();
		final ObjectMapper mapper = new ObjectMapper();

		String output = "";
		try {
			mapper.writeValue(out, values);

			final byte[] data = ((ByteArrayOutputStream) out).toByteArray();
			output = new String(data);
			System.out.println(output);
		} catch (JsonGenerationException e) {
			_logger.error(e.getMessage());
		} catch (JsonMappingException e) {
			_logger.error(e.getMessage());
		} catch (IOException e) {
			_logger.error(e.getMessage());
		}

		return output;
	}

	/*
	 * JSON Helper Methods/Converters
	 */
	public static String fixArrayField(String json, String... fields) {
		for (String field : fields) {
			json = json.replace("\"" + field + "\":{}", "\"" + field + "\":[]");
		}

		return json;
	}
}
