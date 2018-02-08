package se.qxx.protodb.test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;

public class TestBase {
	
	protected static Collection<Object[]> getParams() {
		return Arrays.asList(TestConstants.TEST_PARAMS);
	}
	
	protected static Collection<Object[]> getParams(String paramsProperty) {
		if (System.getProperties().containsKey(paramsProperty)) {
			return readParamsFile(System.getProperty(paramsProperty));
		}
		else {
			return Arrays.asList(TestConstants.TEST_PARAMS);
		}
	}

	/***
	 * Reads test params from file.
	 * Each row defines arguments for a parameterized class
	 * Each object should be separated by semi-colon;
	 * @param filename
	 * @return
	 */
	protected static Collection<Object[]> readParamsFile(String filename) {
		File file = new File(filename);
		Collection<Object[]> result = new ArrayList<Object[]>();
		FileReader fr = null;

		try {
			if (file.exists()) {
				fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				StringBuilder sb = new StringBuilder();
				String line;
				
				while ((line = br.readLine()) != null) {
					result.add(
						StringUtils.split(line, ";"));
				}
				
				return result;
			}
		
		}
		catch (IOException e) {
		}
		finally {
			try {
				if (fr != null)
					fr.close();
			} catch (IOException e) {}
		}
		
		return Arrays.asList(TestConstants.TEST_PARAMS);
	}
}
