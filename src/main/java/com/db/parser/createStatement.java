package com.db.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.db.storageManager.FieldType;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Schema;
import com.db.storageManager.SchemaManager;

public class createStatement {
	
	public createStatement() {
		// default constructor		
	}
	
	// create statement
	public Relation parseCreateStatement(MainMemory mem, String statement, SchemaManager schema_manager) {
		String regexValue = "^\\s*create\\s+table\\s+([a-z][0-9a-z]*)\\s*\\(([a-z][0-9a-z]*\\s+(?:STR20|INT)\\s*(?:,\\s*[a-z][0-9a-z]*\\s+(?:INT|STR20))*\\s*)\\)$";
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		// if the string is matched
		if (match.find()) {
			//System.out.println("value is " + match.group(1) + " " + match.group(2) );
			String relation_name = match.group(1).trim();
			
			// list for the field types
			ArrayList<FieldType> field_types = new ArrayList<FieldType>();
			
			// list for the field names
			ArrayList<String> field_names = new ArrayList<String>();
			
			// process the field names and field types
			getAttribute(match.group(2), field_names, field_types);
			
			
			// create a tuple with given values
			Relation relation_reference = createTable(relation_name, schema_manager, field_names, field_types);
			
			//printRelation(relation_reference);
			
			return relation_reference;
		}	
		return null;
	}
	
	public ArrayList<String> getAttribute(String str, ArrayList<String> field_names, ArrayList<FieldType> field_types) {
		
		String[] names;
		// remove all the spaces before and after the given string
		String trimed = str.trim();
		
		// get all the tokens by spliting by commma
		String[] arr = trimed.split(",");
		
		// remove all the spaces before and after each token
		for (int i = 0; i < arr.length; i++) {
			arr[i] = arr[i].trim();
			names = arr[i].split(" ");
			//System.out.println(Arrays.toString(names));
			field_names.add(names[0].trim());
			
			if (names[1].trim().toLowerCase().equals("str20")) {
				field_types.add(FieldType.STR20);
			} else {
				field_types.add(FieldType.INT);			
			}
		}
		//for (String strin : field_names){
			//System.out.println(strin);
	    //}
		//System.out.println(field_names.size());
		//System.out.println(field_types.size());
		//System.out.println(Arrays.toString(arr));
		return field_names;
	}	
		
	// create a relation
	public static Relation createTable(String relation_name, SchemaManager schema_manager, ArrayList<String> field_names,
			ArrayList<FieldType> field_types) {
		//System.out.println("In createTable");
		//for (FieldType strin : field_types){
			//System.out.println("values of field types in create table:" + strin);
		//}
		// create a schema with given field names and field types
		Schema schema = new Schema(field_names, field_types);

		// create a relation with the given schema and relation name
		Relation relation_reference = schema_manager.createRelation(relation_name, schema);
		//printRelation(relation_reference);
		return relation_reference;
	}
	
	// Print the information about the Relation
	public static void printRelation(Relation relation_reference) {
		System.out.print("The table has name " + relation_reference.getRelationName() + "\n");
		System.out.print("The table has schema: ........" + "\n");
		System.out.print(relation_reference.getSchema() + "\n");
		System.out.print("The table currently have " + relation_reference.getNumOfBlocks() + " blocks" + "\n");
		System.out.print("The table currently have " + relation_reference.getNumOfTuples() + " tuples" + "\n" + "\n");
	}
}


