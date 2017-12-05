package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;
import com.db.storageManager.Tuple;

public class NaturalJoin {
	
	
	public static ArrayList<Tuple> naturalJoin(MainMemory mem, SchemaManager schema_manager, String relationOne, String relationTwo, String joinAttribute) {
		
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
			//System.out.println("OnePassNaturalJoin!!");
			//System.out.println("tsble1 size: "+tableOneSize);
			//System.out.println("tsble2 size: "+tableTwoSize);


			// use one pass natural join algorithm
			OnePassNaturalJoin onePassJoin = new OnePassNaturalJoin();
			result = OnePassNaturalJoin.naturalJoin(mem, schema_manager, relationOne, relationTwo, joinAttribute);
		}
		else { // both tables dont fit in the main memory
			//System.out.println("TwoPassNaturalJoin!!");
			//System.out.println("tsble1 size: "+tableOneSize);
			//System.out.println("tsble2 size: "+tableTwoSize);
			TwoPassSortBasedNaturalJoin twoPassJoin = new TwoPassSortBasedNaturalJoin();
			result = twoPassJoin.twoPassSortBasedNaturalJoin(relationOne, relationTwo, joinAttribute, schema_manager, mem);
		}

		return result;
	}
}
