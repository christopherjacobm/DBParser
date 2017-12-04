package com.db.operations;

import java.util.ArrayList;
import java.util.Collections;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Tuple;

public class Sorting {
	
	// relation does not fit in the main memory
	public static ArrayList<Tuple> twoPassSorting(Relation relation, MainMemory mem, ArrayList<String> sortBy) {
		
		// sort the sublists of the relation
		CommonHelper.phaseOne(mem, relation, sortBy);
		
		// get the starting blocks number of each sublist as an arraylist
		// eg: if relation.getNumOfBlocks = 45, sublist = <0, 10, 20, 30, 40>
		ArrayList<Integer> sublist = CommonHelper.getSublist(relation, mem);
		
		// to store the number of read blocks in each sublist
		int[] blocksRead = new int[sublist.size()];
		
		// number of blocks in the last sublist, in other sublists there are exactly 10 blocks
		// eg: if relation.getNumOfBlocks = 45, num_blocks_last_sublist = 5 
		int num_blocks_last_sublist = CommonHelper.getLastSublistBlocksCount(relation, mem);
		
		// clean out the main memory
		mem = CommonHelper.clearMem(mem);
		
		// array to hold the sublists blocks
		ArrayList<ArrayList<Tuple>> relationTuples = new ArrayList<ArrayList<Tuple>>();
		
		// array for the results
		ArrayList<Tuple> result = new ArrayList<Tuple>();
				
		// read one block from each sublist in each relation into the main memory and store tuples 
		relationTuples = CommonHelper.readBlockFromSublist(sublist, relation, mem, relationTuples, 0, blocksRead);
		
		// sort all the tuples
		for(int i = 0; i < relation.getNumOfTuples(); i++) {
			// if the blocks in memory are empty, bring in the next block from disk
			CommonHelper.ifEmptyReadNextBlock(sublist, relationTuples, relation, blocksRead, mem, num_blocks_last_sublist, 0);
			
			// find the small tuple in each block in the memory
			ArrayList<Tuple> smallTuples = CommonHelper.smallestTuple(sublist, relationTuples, null, sortBy);
		
			// find the smallest tuple from the collection of small tuples
			Tuple smallestTuple = Collections.min(smallTuples, new CompareTuplesSort(sortBy));
			
			// send the smallest tuple to the output
			result.add(smallestTuple);
			
			// find the sublist of the smallest tuple and remove the smallest tuple
			int sublist_num = smallTuples.indexOf(smallestTuple);
			
			// remove the smallest tuple
			relationTuples.get(sublist_num).remove(smallestTuple);
		}
			
		return result;
	}
	
	// when a relation fits in the main memory
	public static ArrayList<Tuple> onePassSorting(Relation relation, MainMemory mem, ArrayList<String> sortBy){
		
		// number of blocks in relation
		int num_blocks = relation.getNumOfBlocks();
		
		// read the blocks from the disk to the main memory
		relation.getBlocks(0,  0, num_blocks);
		
		// store the read blocks in an arraylist
		ArrayList<Tuple> relationTuples = mem.getTuples(0, num_blocks);
		
		// sort all the tuples
		Collections.sort(relationTuples, new CompareTuplesSort(sortBy));
		
		return relationTuples;
	}
}
