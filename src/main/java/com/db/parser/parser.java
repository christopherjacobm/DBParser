package com.db.parser;

import com.db.storageManager.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class parser {
	static SchemaManager schema_manager;

	public static void main(String[] args) {
		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		schema_manager = new SchemaManager(mem, disk);

		// String[] tokens= lex("create TABLE course ( sid INT , homework INT , project
		// INT , exam INT , grade STR20 )");
		//String[] tokens = lex("create TABLE t1 ( id INT , name STR20 )");
		
		//Relation tableName = parse(tokens);
		
		createStatement create = new createStatement();
		create.parseCreateStatement(mem, "create TABLE tablename(name INT, id str20)", schema_manager);
		getFirstWord("select TABLE tablename(name INT, id str20)");
		
		// process the insert statement
		//InsertStatement insert = new InsertStatement();
		//insert.parseInsertStatement(tableName, mem, "insert into tablename(id, name) VALUES(1, \"Sukhdeep\")");
	}
	
	// select the operation using
	private static void getFirstWord(String statement) {
		String regexValue = "^\\s*(create|insert|select|drop|delete)\\s+";
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		// if the string is matched
		if (match.find()) {
			System.out.println("value is " + match.group(1));
		}
		
	}

	public static String[] lex(String str) {
		String arr[] = str.split(" "); // can't always split by spaces in case of comma-separated attributes
		// for (String temp : arr) {
		// System.out.println(temp);
		// }
		return arr;
	}

	public static Relation parse(String[] arr) {
		switch (arr[0]) {
		case "create":
			String[] createArr = Arrays.copyOfRange(arr, 2, arr.length);
			Relation ref = create(createArr);
			return ref;
			
		case "insert":
			// String[] insertArr = Arrays.copyOfRange(arr, 2, arr.length);
			// insert(insertArr);
			//Matcher match = parseInsertStatement("insert into tablename(name,    class,   id) VALUES(\"Sukhdeep\", null, 1)");
			//createTuple(ref, e, MainMemory mem, String[] attributeNames, String[] attributeValues
			return null;			
		}
		return null;
	}
	
	public static Relation create(String[] arr) {
		System.out.println("create called with array: ");
		for (String str : arr)
			System.out.println(str);
		String relation_name = arr[0];
		ArrayList<String> field_names = new ArrayList<String>();
		ArrayList<FieldType> field_types = new ArrayList<FieldType>();
		if (arr[1].equals("(")) {
			String[] attributeTypeListArr = Arrays.copyOfRange(arr, 2, arr.length - 1);
			attributeTypeList(attributeTypeListArr, field_names, field_types);
		}
		Relation relation_ref = createTable(relation_name, field_names, field_types);
		return relation_ref;
	}
	
	public static void attributeTypeList(String[] arr, ArrayList<String> field_names,
			ArrayList<FieldType> field_types) {
		if (arr.length > 2) {// there are more attributes
			parseFirstFieldNameAndFieldType(arr, field_names, field_types);
			// remove first 3 elements of array and recurse
			String[] attributeTypeListArr = Arrays.copyOfRange(arr, 3, arr.length);
			attributeTypeList(attributeTypeListArr, field_names, field_types);
		} else {// last attribute
			parseFirstFieldNameAndFieldType(arr, field_names, field_types);
		}
	}

	private static void parseFirstFieldNameAndFieldType(String[] arr, ArrayList<String> field_names,
			ArrayList<FieldType> field_types) {
		field_names.add(arr[0]);
		if (arr[1].equals("STR20"))
			field_types.add(FieldType.STR20);
		else if (arr[1].equals("INT"))
			field_types.add(FieldType.INT);
	}

	public static Relation createTable(String relation_name, ArrayList<String> field_names,
			ArrayList<FieldType> field_types) {
		System.out.print("In createTable");
		Schema schema = new Schema(field_names, field_types);

		Relation relation_reference = schema_manager.createRelation(relation_name, schema);

		// Print the information about the Relation
		System.out.print("The table has name " + relation_reference.getRelationName() + "\n");
		System.out.print("The table has schema:" + "\n");
		System.out.print(relation_reference.getSchema() + "\n");
		System.out.print("The table currently have " + relation_reference.getNumOfBlocks() + " blocks" + "\n");
		System.out.print("The table currently have " + relation_reference.getNumOfTuples() + " tuples" + "\n" + "\n");
		return relation_reference;
	}
}
