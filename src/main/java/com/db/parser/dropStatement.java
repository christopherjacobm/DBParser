package com.db.parser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;

public class dropStatement {

	public dropStatement() {
		// default constructor		
	}
	
	// delete statement
	public void parseDeleteStatement(String statement, SchemaManager schema_manager) {
		String regexValue = "^\\s*drop\\s+TABLE\\s+([a-z][0-9a-z]*)\\s*$";	
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		// if the string is matched
		if (match.find()) {
			//System.out.println("value is " + match.group(1) + " " + match.group(1));
			String relation_name = match.group(1).trim();
			Boolean tableDropped = schema_manager.deleteRelation(relation_name);
			//printRelation(schema_manager.getRelation(relation_name));
		}
	}
	
	// Print the information about the Relation
	public void printRelation(Relation relation_reference) {
		System.out.print("The table has name " + relation_reference.getRelationName() + "\n");
		System.out.print("The table has schema: ........" + "\n");
		System.out.print(relation_reference.getSchema() + "\n");
		System.out.print("The table currently have " + relation_reference.getNumOfBlocks() + " blocks" + "\n");
		System.out.print("The table currently have " + relation_reference.getNumOfTuples() + " tuples" + "\n" + "\n");
	}
	
}
