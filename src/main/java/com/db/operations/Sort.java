package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Tuple;

public class Sort {
	public static ArrayList<Tuple> sort(Relation relation, MainMemory mem, ArrayList<String> sortBy) {
		
		// arraylist for the sorted tuples
		ArrayList<Tuple> result;
		
		// relation fits in the main memory
		if(relation.getNumOfBlocks() < mem.getMemorySize()) {
			result = Sorting.onePassSorting(relation, mem, sortBy);
		}
		// relation does not fit in the main memory
		else {
			result = Sorting.twoPassSorting(relation, mem, sortBy);
		}

		return result;
	}
}
