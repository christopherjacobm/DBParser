package com.db.parser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class dropStatement {

	public dropStatement() {
		// default constructor		
	}
	
	// delete statement
	public void parseDeleteStatement(String statement) {
		String regexValue = "^\\s*drop\\s+TABLE\\s+([a-z][0-9a-z]*)\\s+$";
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		// if the string is matched
		if (match.find()) {
			System.out.println("value is " + match.group(1) + " " + match.group(1));
			String relation_name = match.group(1).trim();
		}	
	}
	
}
