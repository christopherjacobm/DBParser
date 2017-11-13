package com.db.parser;

import com.db.storageManager.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
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
		String[] tokens = lex("create TABLE t1 ( id INT , name STR20 )");
		
		Relation ref = parse(tokens);
		parseInsertStatement(ref, mem, "insert into tablename(id, name) VALUES(1, \"Sukhdeep\")");
		//createTuple(ref, mem, match.group(2), match.group(3));
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
			//Matcher match = parseInsertStatement("insert into tablename(name,    class,   id) VALUES(\"raj\", null, 1)");
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

	// insert statement
	public static Matcher parseInsertStatement(Relation ref, MainMemory mem, String statement) {
		String regexValue = "^\\s*insert\\s+into\\s+([a-z][a-z0-9]*)\\s*\\((\\s*[a-z][a-z0-9]*\\s*(?:,\\s*[a-z][a-z0-9]*\\s*)*)\\)\\s+values\\s*\\(\\s*([a-z0-9\"]*\\s*(?:,\\s*[a-z0-9\"]*)*)\\)$";
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		if (match.find()) {
			System.out.println("value is " + match.group(1) + " " + match.group(2) + " " + match.group(3));
			String[] attributeNames = trimAndSplitByComma(match.group(2));
			String[] attributeValues = trimAndSplitByComma(match.group(3));
			//return match;
			createTuple(ref, mem, attributeNames, attributeValues);
		}
		return null;
		
	}

	public static String[] trimAndSplitByComma(String str) {
		String trimed = str.trim();
		String[] arr = trimed.split(",");
		String temp = "";
		for (int i = 0; i < arr.length; i++) {
			arr[i] = arr[i].trim();
			arr[i] = arr[i].replaceAll("\"", "");
		}
		System.out.println(Arrays.toString(arr));
		return arr;
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

	public static void createTuple(Relation relation_reference, MainMemory mem, String[] attributeNames, String[] attributeValues) {

		// Create a Tuple of the Relation using the Relation pointer
		Tuple tuple = relation_reference.createTuple();
		Schema schema = relation_reference.getSchema();
		FieldType type; 
		int value;
		for(int i = 0; i < attributeNames.length; i++) {
			if(attributeValues[i].equals("null") || attributeValues[i].equals("NULL")) {
				
			} else {
				type = schema.getFieldType(attributeNames[i]);
				System.out.println("field type " + type);
				if (type.equals(FieldType.INT)) {
					value = Integer.parseInt(attributeValues[i]);
					tuple.setField(attributeNames[i], value);
				} else {
					tuple.setField(attributeNames[i], attributeValues[i]);
				}
			}
		}
		
		
		// Set up a block in the memory and get a pointer to that empty memory Block
		Random rand = new Random();
		System.out.print("Getting a memory block " + "\n");
		// access to a random memory block
		Block block_reference = mem.getBlock(rand.nextInt(10) + 1);

		// grab an empty memory block
		while (!block_reference.isEmpty()) {
			block_reference = mem.getBlock(rand.nextInt(10) + 1);
		}

		block_reference.appendTuple(tuple);
		printTuple(tuple);

	}

	// Create a tuple: Create a Tuple of the Relation using the Relation pointer. It
	// may sound
	// weird, but you have to store the newly created Tuple to the simulated memory
	// to complete
	// this step. First get a pointer to an empty memory Block, say block 7, using
	// the MainMemory
	// object. Store the created Tuple by ”appending” it to the empty memory Block
	// 7.

	// Print the information about the tuple
	private static void printTuple(Tuple tuple) {

		System.out.print("Created a tuple " + tuple + " of ExampleTable3 through the relation" + "\n");
		System.out.print("The tuple is invalid? " + (tuple.isNull() ? "TRUE" : "FALSE") + "\n");
		Schema tuple_schema = tuple.getSchema();
		System.out.print("The tuple has schema" + "\n");
		System.out.print(tuple_schema + "\n");
		System.out.print("A block can allow at most " + tuple.getTuplesPerBlock() + " such tuples" + "\n");

		System.out.print("The tuple has fields: " + "\n");
		for (int i = 0; i < tuple.getNumOfFields(); i++) {
			if (tuple_schema.getFieldType(i) == FieldType.INT)
				System.out.print(tuple.getField(i) + "\t");
			else
				System.out.print(tuple.getField(i) + "\t");
		}
		System.out.print("\n");

//		System.out.print("The tuple has fields: " + "\n");
//		System.out.print(tuple.getField("id") + "\t");
//		System.out.print(tuple.getField("f2") + "\t");
//		System.out.print(tuple.getField("f3") + "\t");
//		System.out.print(tuple.getField("f4") + "\t");
//		System.out.print("\n" + "\n");
	}
}
