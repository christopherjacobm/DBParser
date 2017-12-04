package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.Block;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;
import com.db.storageManager.Tuple;

public class OnePassNaturalJoin {
	
	// natural join of two tables if the size of the smaller table is less than equal to (memorySize -1)
	public static ArrayList<Tuple> naturalJoin(MainMemory mem, SchemaManager schema_manager, String relationOne, String relationTwo, String joinAttribute) {
		// get relationOne using schemaManager
		Relation tableOne = schema_manager.getRelation(relationOne);
		
		// get relationTwo using schemaManager
		Relation tableTwo = schema_manager.getRelation(relationTwo);
		
		// create a new relation for the output
		Relation result_relation = CommonHelper.createRelation(tableOne, tableTwo, schema_manager);
		
		// find the size of relation one
		int tableOneSize = tableOne.getNumOfBlocks();
		
		// find the size of relation one
		int tableTwoSize = tableTwo.getNumOfBlocks();
				
		// find the smaller table
		Relation smallTable;
		
		// Find the larger table
		Relation largeTable;
		
		if(tableOneSize < tableTwoSize) {
			smallTable = tableOne;
			largeTable = tableTwo;
		}
		else {
			smallTable = tableTwo;
			largeTable = tableOne;
		}
		
		// bring all the blocks of the smaller table into the main mem
		smallTable.getBlocks(0, 0, smallTable.getNumOfBlocks());

		Block largeBlock;
		Block smallBlock;
		ArrayList<Tuple> smallTableTuples;
		ArrayList<Tuple> largeTableTuples;
		ArrayList<Tuple> result = null;
		
		int lastMemBlock = mem.getMemorySize() -1;
		
		// bring block from the larger tuple one by one into the main memory
		for(int i = 0; i < largeTable.getNumOfBlocks(); i++) {
			largeTable.getBlock(0, lastMemBlock);
			largeBlock = mem.getBlock(lastMemBlock);
			largeTableTuples = largeBlock.getTuples();
			for(Tuple largeTuple : largeTableTuples) {
				for(int j = 0; j < smallTable.getNumOfBlocks(); j++) {
					smallBlock = mem.getBlock(j);
					smallTableTuples = smallBlock.getTuples();
					for(Tuple smallTuple : smallTableTuples) {
						compareTupleValues(result, largeTuple, smallTuple, joinAttribute, result_relation);
					}
				}
					
			}
		}
		return result;
	}
	
	// compare two tuples on the given field names, if equal join two tuples
	public static void compareTupleValues(ArrayList<Tuple> result, Tuple largeTuple, Tuple smallTuple, String fieldValue, Relation relation) {
		Tuple joinedTuple;
		String fieldOne = smallTuple.getField(fieldValue).toString();
		String fiedTwo =  largeTuple.getField(fieldValue).toString();
		if(CommonHelper.isStringInt(fieldOne) && CommonHelper.isStringInt(fiedTwo)) {								//field type is int
			if(CommonHelper.stringToInteger(fieldOne) == CommonHelper.stringToInteger(fiedTwo)) {
				joinedTuple = CommonHelper.joinTuples(smallTuple, largeTuple, relation);
				result.add(joinedTuple);		
			}
		}else {
			if(fieldOne.equals(fiedTwo)) {
				joinedTuple = CommonHelper.joinTuples(smallTuple, largeTuple, relation);
				result.add(joinedTuple);	
			}
		}
	}
}
