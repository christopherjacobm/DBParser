package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;
import com.db.storageManager.Tuple;

public class NaturalJoin {
	
	
	public static void naturalJoin(MainMemory mem, SchemaManager schema_manager, String relationOne, String relationTwo) {
		
		// get relationOne using schemaManager
		Relation tableOne = schema_manager.getRelation(relationOne);
		
		// get relationTwo using schemaManager
		Relation tableTwo = schema_manager.getRelation(relationTwo);
		
		// find the size of relation one
		int tableOneSize = tableOne.getNumOfBlocks();
		
		// find the size of relation one
		int tableTwoSize = tableTwo.getNumOfBlocks();
				
		// find the smaller table
		Relation smallerTable;
		
		if(tableOneSize < tableTwoSize) {
			smallerTable = tableOne;
		}
		else {
			smallerTable = tableTwo;
		}
		
		// a list to store the final result
		ArrayList<Tuple> result = null;
				
		// the smaller table fits in the main memory
		if(smallerTable.getNumOfBlocks() < mem.getMemorySize()) {
			// use one pass natural join algorithm
		}
		else { // both tables dont fit in the main memory
			// use two pass natural join algorithm
		}
		
		// print the result		
		System.out.println(tableOne.getSchema().fieldNamesToString());      // TO DO : change the tableOne -------> final relation in the list
		for(int i = 0; i< result.size(); i++) {
			System.out.println(result.get(i));
		}
	}
}
