package com.db.parser;

import java.util.Arrays;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.db.storageManager.Block;
import com.db.storageManager.FieldType;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Schema;
import com.db.storageManager.Tuple;

public class InsertStatement {
	
	public InsertStatement() {
		// default constructor
	}
	
	// insert statement
	public void parseInsertStatement(Relation ref, MainMemory mem, String statement) {
		String regexValue = "^\\s*insert\\s+into\\s+([a-z][a-z0-9]*)\\s*\\((\\s*[a-z][a-z0-9]*\\s*(?:,\\s*[a-z][a-z0-9]*\\s*)*)\\)\\s+values\\s*\\(\\s*([a-z0-9\"]*\\s*(?:,\\s*[a-z0-9\"]*)*)\\)$";
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		// if the string is matched
		if (match.find()) {
			System.out.println("value is " + match.group(1) + " " + match.group(2) + " " + match.group(3));
			String[] attributeNames = trimAndSplitByComma(match.group(2));
			String[] attributeValues = trimAndSplitByComma(match.group(3));
			
			// create a tuple with given values
			createTuple(ref, mem, attributeNames, attributeValues);
		}	
	}
	
	public String[] trimAndSplitByComma(String str) {
		
		// remove all the spaces before and after the given string
		String trimed = str.trim();
		
		// get all the tokens by spliting by commma
		String[] arr = trimed.split(",");
		
		// remove all the spaces before and after each token
		for (int i = 0; i < arr.length; i++) {
			arr[i] = arr[i].trim();
			
			// change "john" to john
			arr[i] = arr[i].replaceAll("\"", "");
		}
		System.out.println(Arrays.toString(arr));
		return arr;
	}
	
	public void createTuple(Relation relation_reference, MainMemory mem, String[] attributeNames, String[] attributeValues) {

		// Create a Tuple of the Relation using the Relation pointer
		Tuple tuple = relation_reference.createTuple();
		
		// get a reference to the schema of the relation
		Schema schema = relation_reference.getSchema();
		
		
		FieldType type; 
		int value;
		
		// throw an error if the number of fields dont match the number of values
		for(int i = 0; i < attributeNames.length; i++) {				
			if(attributeValues[i].equals("null") || attributeValues[i].equals("NULL")) {
				// insert the null value
			} else {			
				// grab the type of the field from the schema of the relation
				type = schema.getFieldType(attributeNames[i]);
				System.out.println("field type " + type);			
				// if the field type is int, change the field value from string to int
				if (type.equals(FieldType.INT)) {
					value = Integer.parseInt(attributeValues[i]);			
					// set the value of a field in a tuple
					tuple.setField(attributeNames[i], value);
				} else {
					// set the value of a field in a tuple
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

		// add a tuple to the memory block
		block_reference.appendTuple(tuple);
		
		// print the added tuple
		printTuple(tuple);

	}
	
	// Print the information about the tuple
	private void printTuple(Tuple tuple) {
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
	}

}
