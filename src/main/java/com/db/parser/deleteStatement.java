package com.db.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.db.storageManager.Block;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;

public class deleteStatement {

	public deleteStatement() {
		// default constructor
	}
	
	// delete statement
	public void parseDeleteStatement(String statement, SchemaManager schema_manager, MainMemory mem) {
		String regexValue = "^\\s*delete\\s+from\\s+([a-z][0-9a-z]*)\\s*$";
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		// if the string is matched
		if (match.find()) {
			//System.out.println("value is " + match.group(1));
			String relation_name = match.group(1).trim();
			deleteStatement(relation_name, schema_manager, mem);
		}
	}
	
	public void deleteStatement(String relation_name, SchemaManager schema_manager, MainMemory mem) {
		
		Block block_reference;
		Relation relation_reference = schema_manager.getRelation(relation_name);
		int numBlocks = relation_reference.getNumOfBlocks();
		relation_reference.getBlocks(0,0,numBlocks);     // TODO if the relation does not fit in the main memory
		block_reference=mem.getBlock(0);
	    block_reference.clear(); //clear the block
	    relation_reference.setBlocks(0, 0, numBlocks);
	    
	    //System.out.print("Deleting the last block of the relation to remove trailing space" + "\n");
	    relation_reference.deleteBlocks(relation_reference.getNumOfBlocks()-1);
	    //System.out.print("Now the relation contains: " + "\n");
	    //System.out.print(relation_reference + "\n" + "\n");
	    //System.out.print("*****************************************");
	    //printRelation(schema_manager.getRelation(relation_name));
	    
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
