package com.db.operations;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;

public class CrossJoin {
	
	public static void crossJoin(Relation relation_one, Relation relation_two, MainMemory mem) {
		Relation smallerRelation = null;
		
		// find a smaller relation
		if(relation_one.getNumOfBlocks() < relation_two.getNumOfBlocks()) {
			smallerRelation = relation_one;
		}else {
			smallerRelation = relation_two;
		}
		
		// if the smaller relation fits in the main memory
		if(smallerRelation.getNumOfBlocks() < mem.getMemorySize() -1) {
			// result = onePassCrossJoin(relation_one, relation_two);
		}
		else {   // the smaller relation does not fit in the main memory
			// result = twoPassNaturalJoin(relation_one, relation_two);
		}
	}
	
	public static void twoPassCrossJoin(Relation relation_one, Relation relation_two) {
		          
	}

}
