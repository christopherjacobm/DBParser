package com.db.operations;

import java.util.ArrayList;
import java.util.Collections;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Tuple;

public class FindDistinct {

	// relation dont fit in the main memory
	public static ArrayList<Tuple> twoPassFindDistinct(Relation relation, MainMemory mem, ArrayList<String> fieldNames) {
		
		// sort the sublists of the relation
		CommonHelper.phaseOne(mem, relation, fieldNames);
		
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
		
		// read one block from each sublist of a relation into the main memory and store tuples 
		relationTuples = CommonHelper.readBlockFromSublist(sublist, relation, mem, relationTuples, 0, blocksRead);
		
		// a temp tuple to compare with other tuples
		Tuple compareTuple = null;
		
		// sort all the tuples
		for(int i = 0; i < relation.getNumOfTuples(); i++) {
			// if the blocks in memory are empty, bring in the next block from disk
			CommonHelper.ifEmptyReadNextBlock(sublist, relationTuples, relation, blocksRead, mem, num_blocks_last_sublist, 0);
			
			// find the small tuple in each block in the memory
			ArrayList<Tuple> smallTuples = CommonHelper.smallestTuple(sublist, relationTuples, null, fieldNames);
		
			// find the smallest tuple from the collection of small tuples
			Tuple smallestTuple = Collections.min(smallTuples, new CompareTuplesSort(fieldNames));
			
			// find the sublist of the smallest tuple
			int sublist_num = smallTuples.indexOf(smallestTuple);
						
			// only add to the final output if distinct 
			if(!areDistinctColumnsEqual(fieldNames, smallestTuple, compareTuple)) {
				result.add(smallestTuple);
				compareTuple = smallestTuple;
			}

			// remove the smallest tuple
			relationTuples.get(sublist_num).remove(smallestTuple);
		}
		return result;
	}
	
	// check if the tuples are the same
	public static Boolean areDistinctColumnsEqual(ArrayList<String> fieldNames, Tuple tupleOne, Tuple tupleTwo) {
		if((tupleOne== null) || (tupleTwo == null)) {
			return false;
		}
		for(String field : fieldNames) {
			if(!(tupleOne.getField(field).toString().equals(tupleTwo.getField(field).toString()))) {
				return false;
			}
		}
		return true;		
	}
	
	// relation fits in the main memory
	public static ArrayList<Tuple> onePassFindDistinct(Relation relation, MainMemory mem, ArrayList<String> fieldNames) {
		// number of blocks in a relation
		int num_blocks = relation.getNumOfBlocks();
		
		// read the blocks from the disk to the main memory
		relation.getBlocks(0,  0, num_blocks);
		
		// store the read blocks in an arraylist
		ArrayList<Tuple> relationTuples = mem.getTuples(0, num_blocks);
		
		// array for the results
		ArrayList<Tuple> result = new ArrayList<Tuple>();
				
		Tuple compareTuple = null;
		Tuple smallestTuple = null;
		System.out.println("fieldnames: "+fieldNames);
		
		// while the is not empty
		while(relationTuples.size() != 0) {
			smallestTuple = Collections.min(relationTuples, new CompareTuplesSort(fieldNames));

			System.out.println("smallestTuple: "+smallestTuple.toString(false));
			if (compareTuple!=null) System.out.println("compareTuple: "+compareTuple.toString(false));

			
			// only add to the final output if distinct 
			if(!areDistinctColumnsEqual(fieldNames, smallestTuple, compareTuple)) {
				result.add(smallestTuple);
				compareTuple = smallestTuple;
				System.out.println("inside if");
			}
	
			// remove the smallest tuple
			relationTuples.remove(smallestTuple);
			
		}
		System.out.println("In onePassFindDistinct,result: "+result);
		return result;
		
	}
}
