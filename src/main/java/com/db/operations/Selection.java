package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.Block;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Tuple;

public class Selection {
	
	public Selection() {
		// default constructor
	}

	
	public void selectTuples(Relation relation_referenec, MainMemory mem) {
		int num_blocks = relation_referenec.getNumOfBlocks();
		Boolean diskToMem = false;
		if(num_blocks <= 9) {			// leave one block of memory empty for the output
			diskToMem = relation_referenec.getBlocks(0, 0, num_blocks);
		}
		
		if(diskToMem) {
			for(int i = 0; i < num_blocks; i++) {
				Block b = mem.getBlock(0);
		        ArrayList<Tuple> tuples= b.getTuples();
			}			
		}
	}
}
