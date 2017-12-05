package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;
import com.db.storageManager.Tuple;

public class CrossJoin {
	
	public static ArrayList<Tuple> crossJoin(Relation relation_one, Relation relation_two, MainMemory mem, SchemaManager schema_manager) {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		
		Relation smallerRelation;
		Relation largerRelation;
		
		// find a smaller relation
		if(relation_one.getNumOfBlocks() < relation_two.getNumOfBlocks()) {
			smallerRelation = relation_one;
			largerRelation = relation_two;
		}else {
			smallerRelation = relation_two;
			largerRelation = relation_one;
		}
		
		// if the smaller relation fits in the main memory
		if(smallerRelation.getNumOfBlocks() < mem.getMemorySize() -1) {
			// result = onePassCrossJoin(smallerRelation, largerRelation);
		}
		else {   // the smaller relation does not fit in the main memory
			result = twoPassCrossJoin(relation_one, relation_two, schema_manager, mem);
		}
		return result;
	}
	
	public static ArrayList<Tuple> twoPassCrossJoin(Relation relationOne, Relation relationTwo, SchemaManager schema_manager, MainMemory mem) {
		// create a new relation for the output
		Relation output_relation = CommonHelper.createRelation(relationOne, relationTwo, schema_manager, "TwoPassCrossJoin");
	
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		
		for(int i = 0; i < relationOne.getNumOfBlocks(); i++) {
			 relationOne.getBlock(i, 0);							// get one block of relation one from the memory
			 //relationOne.getBlocks(i, 0, 5);
			 for(int j = 0; j < relationTwo.getNumOfBlocks(); j++) {
				 relationOne.getBlock(j, 1);
				 //relationOne.getBlocks(j, 5, 5);
				 for(Tuple tupleOne : mem.getBlock(0).getTuples()) {
					 for(Tuple tupleTwo : mem.getBlock(1).getTuples()) {			 
						 result.add(CommonHelper.joinTuples(tupleOne, tupleTwo, output_relation, null));
					 }
				 }
			 }
			 
		}
		return result;
	}
	
	
//	// bring in 5  blocks at a time
//	public static ArrayList<Tuple> twoPassCrossJoin2(Relation relationOne, Relation relationTwo, SchemaManager schema_manager, MainMemory mem) {
//		// create a new relation for the output
//		Relation output_relation = CommonHelper.createRelation(relationOne, relationTwo, schema_manager, "TwoPassCrossJoin");
//	
//		ArrayList<Tuple> result = new ArrayList<Tuple>();
//		
//		for(int i = 0; i < relationOne.getNumOfBlocks(); i = i+5) {
//			 // get 5 blocks of relation one from the memory
//			 relationOne.getBlocks(i, 0, 5);
//			 for(int j = 0; j < relationTwo.getNumOfBlocks(); j= j+5) {
//				 relationOne.getBlocks(j, 5, 5);  ;
//				 for(Tuple tupleOne : mem.getTuples(0, 5)) {
//					 for(Tuple tupleTwo : mem.getTuples(5, 5)) {			 
//						 result.add(CommonHelper.joinTuples(tupleOne, tupleTwo, output_relation, null));
//					 }
//				 }
//			 }
//			 
//		}
//		return result;
//	}
	
	public static ArrayList<Tuple> OnePassCrossJoin(Relation smallRelation, Relation largerRelation, SchemaManager schema_manager, MainMemory mem) {
		// create a new relation for the output
		Relation output_relation = CommonHelper.createRelation(smallRelation, largerRelation, schema_manager, "OnePassCrossJoin");
	
		ArrayList<Tuple> result = new ArrayList<Tuple>();
	
		// rewad all the blocks of the small relation in the memory
		int smallRelation_size = smallRelation.getNumOfBlocks();
		smallRelation.getBlocks(0, 0, smallRelation_size);	
		
		int largeRelation_size = largerRelation.getNumOfBlocks();
		
		// mem blocks that can be used to bring blocks of large relation
		int mem_blocks_unused = mem.getMemorySize() - smallRelation_size;
		
		int last_blocks = largeRelation_size % mem_blocks_unused;
		
		int loop_size = largeRelation_size / mem_blocks_unused;
		ArrayList<Tuple> large_tuples;
		ArrayList<Tuple> small_tuples = mem.getTuples(0, smallRelation_size);
		int blocks_read = 0;
	//	largerRelation.getBlocks(relation_block_index, memory_block_index, num_blocks)
		for(int i = 0; i < loop_size; i++) {
			// get one block of relation one from the memory
			largerRelation.getBlocks(blocks_read, smallRelation_size, mem_blocks_unused);
			large_tuples = mem.getTuples(smallRelation_size, mem_blocks_unused);
//			for (Tuple tupleSmall : small_tuples) {
//				for (Tuple tupleLarge : large_tuples) {
//					result.add(CommonHelper.joinTuples(tupleSmall, tupleLarge, output_relation, null));
//				}
//			}
			result = crossJoinHelper(small_tuples, large_tuples, result, output_relation);
			 blocks_read += mem_blocks_unused;
			 
		}
		if(last_blocks > 0) {
			largerRelation.getBlocks(blocks_read, smallRelation_size, last_blocks);
			large_tuples = mem.getTuples(smallRelation_size, last_blocks);
//			for (Tuple tupleSmall : small_tuples) {
//				for (Tuple tupleLarge : large_tuples) {
//					result.add(CommonHelper.joinTuples(tupleSmall, tupleLarge, output_relation, null));
//				}
//			}
			result = crossJoinHelper(small_tuples, large_tuples, result, output_relation);
		}
		return result;
	}
	
	public static ArrayList<Tuple> twoPassCrossJoinOptimized(Relation relationOne, Relation relationTwo, SchemaManager schema_manager, MainMemory mem) {
		// create a new relation for the output
		Relation output_relation = CommonHelper.createRelation(relationOne, relationTwo, schema_manager, "TwoPassCrossJoin");
	
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		// rewad all the blocks of the small relation in the memory
		int r1_size = relationOne.getNumOfBlocks();
		int r2_size = relationTwo.getNumOfBlocks();
		
		Relation larger = null;
		Relation smaller = null;
		
		int loop_outer_top = 0;
		int remaining_blocks_top = 0;
		int loop_inner_bottom = 0;
		int remaining_blocks_bottom = 0;
		if(r1_size > r2_size) {
			larger = relationOne;
			smaller = relationTwo;
			loop_outer_top = r1_size / 5;
			remaining_blocks_top = r1_size % 5;
			loop_inner_bottom = r2_size / 5;
			remaining_blocks_bottom = r2_size % 5;
			
		} else {
			loop_outer_top = r2_size / 5;
			remaining_blocks_top = r2_size % 5;
			loop_inner_bottom = r1_size / 5;
			remaining_blocks_bottom = r1_size % 5;
			larger = relationTwo;
			smaller = relationOne;
		}
		
		int read_large_blocks = 0;
		int read_small_blocks = 0;
		ArrayList<Tuple> large_tuples = null;
		ArrayList<Tuple> small_tuples = null;
		for(int i = 0; i < loop_outer_top; i++) {
			//larger.getBlocks(relation_block_index, memory_block_index, num_blocks)
			larger.getBlocks(read_large_blocks, 0, 5);
			large_tuples = mem.getTuples(0, 5);
			
			for(int j = 0; j < loop_inner_bottom; j++) {
				smaller.getBlocks(read_small_blocks, 5, 5);
				small_tuples = mem.getTuples(5, 5);
				result = crossJoinHelper(large_tuples, small_tuples, result, output_relation);
				read_small_blocks += 5;
			}
			
			if(remaining_blocks_bottom > 0) {
				smaller.getBlocks(read_small_blocks, 5, remaining_blocks_bottom );
				small_tuples = mem.getTuples(5, remaining_blocks_bottom);
				result = crossJoinHelper(large_tuples, small_tuples, result, output_relation);
			}
			read_large_blocks += 5;
			 
		}
		
		if(remaining_blocks_top > 0) {
			larger.getBlocks(read_large_blocks, 0, remaining_blocks_top);
			large_tuples = mem.getTuples(0, remaining_blocks_top);
			
			for(int j = 0; j < loop_inner_bottom; j++) {
				smaller.getBlocks(read_small_blocks, 5, 5);
				small_tuples = mem.getTuples(5, 5);
				result = crossJoinHelper(large_tuples, small_tuples, result, output_relation);
				read_small_blocks += 5;
			}
			
			if(remaining_blocks_bottom > 0) {
				smaller.getBlocks(read_small_blocks, 5, remaining_blocks_bottom );
				small_tuples = mem.getTuples(5, remaining_blocks_bottom);
				result = crossJoinHelper(large_tuples, small_tuples, result, output_relation);
			}
		}
		
		
		return result;
	}
	
	public static ArrayList<Tuple> crossJoinHelper(ArrayList<Tuple> top_tuples, ArrayList<Tuple> bottom_tuples, ArrayList<Tuple> result, Relation output_relation) {
		for (Tuple tupleSmall : top_tuples) {
			for (Tuple tupleLarge : bottom_tuples) {
				result.add(CommonHelper.joinTuples(tupleSmall, tupleLarge, output_relation, null));
			}
		}
		return result;
	}

}
