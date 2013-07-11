package com.ciq.qless.java.test.utils;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ciq.qless.java.utils.JsonHelper;

public class JsonHelperTest {

	@Test
	public void testParseListString() {

		fail("Not yet implemented");
	}

	@Test
	public void testParseListListOfStringClassOfEClassOfT() {
		fail("Not yet implemented");
	}

	@Test
	public void testParseMap() {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("enddate", "2013-06-27T19:36:23.760Z");
		data.put("startdate", "2000-01-01");
		data.put("offerid", "afa1fe98-d573-4ad9-9442-7735ddd509d2");
		data.put("mappingid", 57);
		data.put("ruleid", 11);
		data.put(
				"url",
				"http://www.famousfootwear.com/en-US/Product/94523-1028839/Perry+Ellis/Grey_Blue/Mens+Kenneth.aspx");

		Map<String, Object> rawData = new HashMap<String, Object>();
		rawData.put("ProductName", "Men's Kenneth");
		rawData.put("ProductPrice", "$39.99 &nbsp;");
		data.put("raw", rawData);

		Map<String, Object> timingData = new HashMap<String, Object>();
		timingData.put("total", 0.097);
		timingData.put("fetch", 0.012);
		timingData.put("parse", 0.084);
		data.put("timing", timingData);

		Map<String, Object> resultData = new HashMap<String, Object>();
		resultData.put("ProductPrice", 3999);
		resultData.put("Location", "OnPage");
		resultData.put("SourceSystem", 1);
		resultData.put("ProductName", "Men's Kenneth");
		data.put("results", resultData);

		String output = JsonHelper.createJSON(data);
		fail("Not yet implemented");
	}

	@Test
	public void testParseMapFirstObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testCreateJSON() {
		fail("Not yet implemented");
	}

	@Test
	public void testFixArrayField() {
		fail("Not yet implemented");
	}

}
