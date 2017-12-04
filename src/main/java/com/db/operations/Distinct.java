package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Tuple;

public class Distinct {

	public static ArrayList<Tuple> distinct(Relation relation, MainMemory mem, ArrayList<String> fieldNames) {
		// arraylist for the sorted tuples
		ArrayList<Tuple> result;
		
		// relation fits in the main memory
		if(relation.getNumOfBlocks() < mem.getMemorySize()) {
			result = FindDistinct.onePassFindDistinct(relation, mem, fieldNames);
		}
		// relation does not fit in the main memory
		else {
			result = FindDistinct.twoPassFindDistinct(relation, mem, fieldNames);
		}

		return result;
	}
}
