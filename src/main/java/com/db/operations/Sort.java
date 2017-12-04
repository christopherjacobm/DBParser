package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Tuple;

public class Sort {
	public static void sort(Relation relation, MainMemory mem, ArrayList<String> sortBy) {
		
		// arraylist for the sorted tuples
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		
		// relation fits in the main memory
		if(relation.getNumOfBlocks() < mem.getMemorySize()) {
			result = Sorting.onePassSorting(relation, mem, sortBy);
		}
		// relation does not fit in the main memory
		else {
			result = Sorting.twoPassSorting(relation, mem, sortBy);
		}
		
		// print the sorted tuples
		System.out.println(result.get(0).getSchema().fieldNamesToString());
		for(Tuple tuple : result) {
			System.out.println(tuple);
		}
	}
}
