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
		int n = rand.nextInt(10) + 1;
		Block block_reference = mem.getBlock(n);

		// grab an empty memory block
		while (!block_reference.isEmpty()) {
			block_reference = mem.getBlock(rand.nextInt(10) + 1);
		}

		// add a tuple to the memory block
		block_reference.appendTuple(tuple);
		
		// append the tuple to the end of the given relation using memory block n as output buffer
	    appendTupleToRelation(relation_reference,mem,n,tuple);
		
		// print the added tuple
		printTuple(tuple);
		
		printRelation(relation_reference);

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
	
	// An example procedure of appending a tuple to the end of a relation
	  // using memory block "memory_block_index" as output buffer
	  private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
	    Block block_reference;
	    if (relation_reference.getNumOfBlocks()==0) {
	      System.out.print("The relation is empty" + "\n");
	      System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
	      block_reference=mem.getBlock(memory_block_index);
	      block_reference.clear(); //clear the block
	      block_reference.appendTuple(tuple); // append the tuple
	      System.out.print("Write to the first block of the relation" + "\n");
	      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
	    } else {
	      System.out.print("Read the last block of the relation into memory block 5:" + "\n");
	      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
	      block_reference=mem.getBlock(memory_block_index);

	      if (block_reference.isFull()) {
	        System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
	        block_reference.clear(); //clear the block
	        block_reference.appendTuple(tuple); // append the tuple
	        System.out.print("Write to a new block at the end of the relation" + "\n");
	        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
	      } else {
	        System.out.print("(The block is not full: Append it directly)" + "\n");
	        block_reference.appendTuple(tuple); // append the tuple
	        System.out.print("Write to the last block of the relation" + "\n");
	        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
	      }
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
