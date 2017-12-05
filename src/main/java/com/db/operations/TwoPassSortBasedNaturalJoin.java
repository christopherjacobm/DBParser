package com.db.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.db.storageManager.Block;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;
import com.db.storageManager.Tuple;

public class TwoPassSortBasedNaturalJoin {

	
//  sublists are sorted by the join attributes.
//	REPEAT
//	  IF Rmin = Smin THEN
//	     collect all Rmin-tuples and all 
//	     Smin-tuples, and send their join 
//	     to the output; 
//	     delete all Rmin and Smintuples;
//	  ELSE delete the smaller;
	
	public ArrayList<Tuple> twoPassSortBasedNaturalJoin(String relationOneName, String relationTwoName, String joinAttribute, SchemaManager schema_manager, MainMemory mem) {
		// arrays to hold the sublists blocks
		ArrayList<ArrayList<Tuple>> relationOneTuples = new ArrayList<ArrayList<Tuple>>();
		ArrayList<ArrayList<Tuple>> relationTwoTuples = new ArrayList<ArrayList<Tuple>>();
		
		// get relationOne using schemaManager
		Relation relationOne = schema_manager.getRelation(relationOneName);
		
		// get relationTwo using schemaManager
		Relation relationTwo = schema_manager.getRelation(relationTwoName);
		
		// sort the relation blocks(sublists) by the join attribute
		ArrayList<String> sortBy = new ArrayList<String>();
		sortBy.add(joinAttribute);
		
		// sort the sublists of each relation
		CommonHelper.phaseOne(mem, relationOne, sortBy);
		CommonHelper.phaseOne(mem, relationTwo, sortBy);

		// clean out the main memory
		mem = CommonHelper.clearMem(mem);

		// get the starting blocks number of each sublist as an array
		ArrayList<Integer> sublistOne = CommonHelper.getSublist(relationOne, mem);

		//System.out.println();
		ArrayList<Integer> sublistTwo = CommonHelper.getSublist(relationTwo, mem);

		// number of blocks in the last sublist, in other sublists there are exactly 10 blocks
		int num_blocks_last_sublist_one = CommonHelper.getLastSublistBlocksCount(relationOne, mem);
		int num_blocks_last_sublist_two = CommonHelper.getLastSublistBlocksCount(relationTwo, mem);

		// to store the number of read blocks in each sublist
		int[] blocksRead_tableOne = new int[sublistOne.size()];
		int[] blocksRead_tableTwo = new int[sublistTwo.size()];

		// read one block from each sublist in each relation into the main memory and store tuples 
		relationOneTuples = CommonHelper.readBlockFromSublist(sublistOne, relationOne, mem, relationOneTuples, 0, blocksRead_tableOne);         					// startingIndex = 0, memory blocks empty
		relationTwoTuples = CommonHelper.readBlockFromSublist(sublistTwo, relationTwo, mem, relationTwoTuples, sublistOne.size(), blocksRead_tableTwo);		    // startingIndex = sublistOne.size(), one block from each sublist of relation one already exist in the memory

//		int temp =0;
//		for(int i = 0; i < relationOneTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
//			 temp += relationOneTuples.get(i).size();
//		}
//		
//		int temp2 =0;
//		for(int i = 0; i < relationTwoTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
//			 temp2 += relationTwoTuples.get(i).size();
//		}
//		System.out.println("temp "+ temp );
//		System.out.println("temp2 "+ temp2 );

		// result of natural join of two tables
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		while(!isSublistEmpty(relationOneTuples) && !isSublistEmpty(relationTwoTuples)) {


			// if the blocks in memory are empty, bring in the next block from disk
//			CommonHelper.ifEmptyReadNextBlock(sublistOne, relationOneTuples, relationOne, blocksRead_tableOne, mem, num_blocks_last_sublist_one, 0);
//			CommonHelper.ifEmptyReadNextBlock(sublistTwo, relationTwoTuples, relationTwo, blocksRead_tableTwo, mem, num_blocks_last_sublist_two, sublistOne.size());

			// find the small tuple in each block in the memory
			ArrayList<Tuple> smallTuples_relationOne = CommonHelper.smallestTuple(sublistOne, relationOneTuples, joinAttribute, null);
			ArrayList<Tuple> smallTuples_relationTwo = CommonHelper.smallestTuple(sublistTwo, relationTwoTuples, joinAttribute, null);

			// pick the smallest tuple of each relation
			Tuple smallestTuple_relationOne =  Collections.min(smallTuples_relationOne,  new CompareTuplesMin(joinAttribute));
			Tuple smallestTuple_relationTwo =  Collections.min(smallTuples_relationTwo,  new CompareTuplesMin(joinAttribute));

			String commonFieldValue_relationOne = getFieldValue(smallestTuple_relationOne, joinAttribute);
			String commonFieldValue_relationTwo = getFieldValue(smallestTuple_relationTwo, joinAttribute);
			
			if(commonFieldValue_relationOne != null && commonFieldValue_relationTwo != null) {
				if(commonFieldValue_relationOne.equals(commonFieldValue_relationTwo)) {
					// find the tuples having the same value of the join attribute as the smallest tuple value in all the sublists
					ArrayList<Tuple> mathcingTuples_relationOne = getMatchingTuples(relationOneTuples, joinAttribute, commonFieldValue_relationOne);
					ArrayList<Tuple> mathcingTuples_relationTwo = getMatchingTuples(relationTwoTuples, joinAttribute, commonFieldValue_relationTwo);
					
					// create a new relation for the output
					Relation output_relation = CommonHelper.createRelation(relationOne, relationTwo, schema_manager, "NaturalJoin");
					
					//joinTuples and return the arraylist of all the resulting tuples
					crossProductTuples(mathcingTuples_relationOne, mathcingTuples_relationTwo, output_relation, joinAttribute, result);
					
					
					
					Boolean keep_min_one = false;
					Boolean keep_min_two = false;
					
					Block tempBlock;
					for(int i = 0; i < sublistOne.size(); i++) {
						tempBlock = mem.getBlock(i);
						if((getSameTuplesCountSingleSegment(commonFieldValue_relationOne, joinAttribute, relationOneTuples.get(i)) == tempBlock.getNumTuples()) ) {
							keep_min_two = true;
							break;
						}
					}
					
					for(int i = 0; i < sublistTwo.size(); i++) {
						tempBlock = mem.getBlock(i+sublistOne.size());
						if((getSameTuplesCountSingleSegment(commonFieldValue_relationTwo, joinAttribute, relationTwoTuples.get(i)) == tempBlock.getNumTuples()) ) {
							keep_min_one = true;
							break;
						}
					}
					
					if(keep_min_one && !keep_min_two) {
						relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
					}
					
					if(!keep_min_one && keep_min_two) {
						relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
					}
					
					if((keep_min_one && keep_min_two) || (!keep_min_one && !keep_min_two)) {
						// delete both Relation one small tuples and relation two small tuples
						relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
						relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
					}
				
				}
				else {					// values dont match, then delete the smallest one from the memory
					if(CommonHelper.isStringInt(commonFieldValue_relationOne) && CommonHelper.isStringInt(commonFieldValue_relationTwo)) {								//field type is int
						if(CommonHelper.stringToInteger(commonFieldValue_relationOne) - CommonHelper.stringToInteger(commonFieldValue_relationOne) > 0) {
							relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
						}
						else {
							relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
						}
					}
					else {
						if((commonFieldValue_relationOne.compareTo(commonFieldValue_relationTwo)) > 0) {
							relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
						}
						else {
							relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
						}
					}
				}
			}

////			System.out.println(relationOneTuples.size());
////			System.out.println(relationTwoTuples.size());
////			System.out.println("temp :" + temp);
//			temp = 0;
//			for(int i = 0; i < relationOneTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
//			//	if(relationOneTuples.get(i) != null) {
//					temp += relationOneTuples.get(i).size();
//			//	}
//			}
//			temp2 =0;
//			for(int i = 0; i < relationTwoTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
//			//	if(relationOneTuples.get(i) != null) { 
//					temp2 += relationTwoTuples.get(i).size();
//			//	}
//			}
//			
//			System.out.println("temp "+ temp );
//			System.out.println("temp2 "+ temp2 );
			CommonHelper.ifEmptyReadNextBlock(sublistOne, relationOneTuples, relationOne, blocksRead_tableOne, mem, num_blocks_last_sublist_one, 0);
			CommonHelper.ifEmptyReadNextBlock(sublistTwo, relationTwoTuples, relationTwo, blocksRead_tableTwo, mem, num_blocks_last_sublist_two, sublistOne.size());
		}
		return result;
	}
	
	// return true if the arraylist of (arralist of tuples )is empty
	public Boolean isListEmpty(ArrayList<ArrayList<Tuple>> list) {
		for(ArrayList<Tuple> tuple :list) {
			if(tuple.size() != 0) {
				return false;
			}
		}
		return true;
	}
	
	// for a given tuple, return the value of a given field if tuple not null
	public String getFieldValue(Tuple tuple, String fieldName) {
		String fieldValue = null;
		if(tuple != null) {
			fieldValue = tuple.getField(fieldName).toString();
		}
		return fieldValue;
	}
	
	// find the tuples having the same attribute value as the matching value
	public ArrayList<Tuple> getMatchingTuples(ArrayList<ArrayList<Tuple>> relationtuples, String joinAttribute, String macthingValue){
		//System.out.println("Inside getMatchingTuples: matching value " +macthingValue );
		//System.out.println("Inside getMatchingTuples: join attribute " +joinAttribute );
		ArrayList<Tuple> matchingTuples = new ArrayList<Tuple>();
		// loop over each sublist
		for(int i = 0; i < relationtuples.size(); i++) {
			// loop over each block of tuples
			for(int j = 0; j < relationtuples.get(i).size(); j++) {
				if(relationtuples.get(i).get(j).getField(joinAttribute).toString().equals(macthingValue)) {
					matchingTuples.add(relationtuples.get(i).get(j));
				}
			}
			
		}
		return matchingTuples;
	}
	
	public void crossProductTuples(ArrayList<Tuple> listOne, ArrayList<Tuple> listTwo, Relation relation, String joinAttribute, ArrayList<Tuple> result){
		for(int i = 0; i < listOne.size(); i++) {
			for(int j = 0; j < listTwo.size(); j++) {
				result.add(CommonHelper.joinTuples(listOne.get(i), listTwo.get(j), relation, joinAttribute));
			}
		}

	}
	
//	public Relation createRelation(Relation relation_one, Relation relation_two, SchemaManager schema_manager) {
//		// field names and field types of relation one
//		ArrayList<String> field_names = relation_one.getSchema().getFieldNames();
//		ArrayList<String> field_names_two = relation_two.getSchema().getFieldNames();
//		
//		// field names and field types of the relation two
//		ArrayList<FieldType> field_types = relation_one.getSchema().getFieldTypes();
//		ArrayList<FieldType> field_types_two = relation_two.getSchema().getFieldTypes();
//		
//		// append all the field names of relation two to relation one field names
//		field_names.addAll(field_names_two);
//
//		// append all the field types of relation two to relation one field types
//		field_types.addAll(field_types_two);
//		
//		Schema schema = new Schema(field_names, field_types);
//		
//		String relation_name = relation_one.getRelationName() + "NaturalJoin" + relation_two.getRelationName();
//		if(schema_manager.relationExists(relation_name)) {
//			schema_manager.deleteRelation(relation_name);
//		}
//
//		Relation relation = schema_manager.createRelation(relation_name, schema);
//		return relation;
//	}
	
	
	// delete the tuples from the listing that have the same field value as the matching value
	public ArrayList<ArrayList<Tuple>> deleteSmallTuples(ArrayList<ArrayList<Tuple>> relationTuples, String matching_value, String field_name) {
//		System.out.print("matching_value : " + matching_value);
//		System.out.print("field_name : " + field_name);
//		List<String> deleteValues = new ArrayList<>();
//		deleteValues.add(matching_value);
       

        
		for(int i = 0; i < relationTuples.size(); i++) {
			//relationTuples.get(i).removeAll(matching_value);
			
			for(int j = 0; j < relationTuples.get(i).size(); j++) {
				if(relationTuples.get(i).get(j).getField(field_name).toString().equals(matching_value)) {
					relationTuples.get(i).remove(relationTuples.get(i).get(j));
					j--;
				}
			}
		}
		return relationTuples;
	}
	
	public static Boolean isSublistEmpty(ArrayList<ArrayList<Tuple>> relationTuples) {
		for(ArrayList<Tuple> list : relationTuples) {
			if(list.size() != 0) {
				return false;
			}
		}
		return true;
	}
	
	// get the same value tuples in a single segment
	public static int getSameTuplesCountSingleSegment(String matchingValue, String fieldName, ArrayList<Tuple> relationTuples) {
		int count = 0;
		for(Tuple tuple : relationTuples) {
			if(tuple.getField(fieldName).toString().equals(matchingValue)) {
				count++;
			}
		}
		return count;
	}
	
	// get the same value tuples in all the segments of a relation
	public static int getSameTuplesCountAllSegments(String matching_value, String field_name, ArrayList<ArrayList<Tuple>> relationTuples) {
		int count = 0;
		for(int i = 0; i < relationTuples.size(); i++) {
			for(int j = 0; j < relationTuples.get(i).size(); j++) {
				if(relationTuples.get(i).get(j).getField(field_name).toString().equals(matching_value)) {
					relationTuples.get(i).remove(relationTuples.get(i).get(j));
				}
			}
		}
		return count;
	}
	
	
}













//package com.db.operations;
//
//import java.util.ArrayList;
//import java.util.Collections;
//
//import com.db.storageManager.Block;
//import com.db.storageManager.MainMemory;
//import com.db.storageManager.Relation;
//import com.db.storageManager.SchemaManager;
//import com.db.storageManager.Tuple;
//
//public class TwoPassSortBasedNaturalJoin {
//
//	
////  sublists are sorted by the join attributes.
////	REPEAT
////	  IF Rmin = Smin THEN
////	     collect all Rmin-tuples and all 
////	     Smin-tuples, and send their join 
////	     to the output; 
////	     delete all Rmin and Smintuples;
////	  ELSE delete the smaller;
//	
//	public ArrayList<Tuple> twoPassSortBasedNaturalJoin(String relationOneName, String relationTwoName, String joinAttribute, SchemaManager schema_manager, MainMemory mem) {
//		// arrays to hold the sublists blocks
//		ArrayList<ArrayList<Tuple>> relationOneTuples = new ArrayList<ArrayList<Tuple>>();
//		ArrayList<ArrayList<Tuple>> relationTwoTuples = new ArrayList<ArrayList<Tuple>>();
//		
//		// get relationOne using schemaManager
//		Relation relationOne = schema_manager.getRelation(relationOneName);
//		
//		// get relationTwo using schemaManager
//		Relation relationTwo = schema_manager.getRelation(relationTwoName);
//		
//		// sort the relation blocks(sublists) by the join attribute
//		ArrayList<String> sortBy = new ArrayList<String>();
//		sortBy.add(joinAttribute);
//		
//		// sort the sublists of each relation
//		CommonHelper.phaseOne(mem, relationOne, sortBy);
//		CommonHelper.phaseOne(mem, relationTwo, sortBy);
//
//		// clean out the main memory
//		mem = CommonHelper.clearMem(mem);
//
//		// get the starting blocks number of each sublist as an array
//		ArrayList<Integer> sublistOne = CommonHelper.getSublist(relationOne, mem);
//
//		//System.out.println();
//		ArrayList<Integer> sublistTwo = CommonHelper.getSublist(relationTwo, mem);
//
//		// number of blocks in the last sublist, in other sublists there are exactly 10 blocks
//		int num_blocks_last_sublist_one = CommonHelper.getLastSublistBlocksCount(relationOne, mem);
//		int num_blocks_last_sublist_two = CommonHelper.getLastSublistBlocksCount(relationTwo, mem);
//
//		// to store the number of read blocks in each sublist
//		int[] blocksRead_tableOne = new int[sublistOne.size()];
//		int[] blocksRead_tableTwo = new int[sublistTwo.size()];
//
//		// read one block from each sublist in each relation into the main memory and store tuples 
//		relationOneTuples = CommonHelper.readBlockFromSublist(sublistOne, relationOne, mem, relationOneTuples, 0, blocksRead_tableOne);         					// startingIndex = 0, memory blocks empty
//		relationTwoTuples = CommonHelper.readBlockFromSublist(sublistTwo, relationTwo, mem, relationTwoTuples, sublistOne.size(), blocksRead_tableTwo);		    // startingIndex = sublistOne.size(), one block from each sublist of relation one already exist in the memory
//
////		int temp =0;
////		for(int i = 0; i < relationOneTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
////			 temp += relationOneTuples.get(i).size();
////		}
////		
////		int temp2 =0;
////		for(int i = 0; i < relationTwoTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
////			 temp2 += relationTwoTuples.get(i).size();
////		}
////		System.out.println("temp "+ temp );
////		System.out.println("temp2 "+ temp2 );
//
//		// result of natural join of two tables
//		ArrayList<Tuple> result = new ArrayList<Tuple>();
//		while(!isSublistEmpty(relationOneTuples) && !isSublistEmpty(relationTwoTuples)) {
//
//
//			// if the blocks in memory are empty, bring in the next block from disk
//			CommonHelper.ifEmptyReadNextBlock(sublistOne, relationOneTuples, relationOne, blocksRead_tableOne, mem, num_blocks_last_sublist_one, 0);
//			CommonHelper.ifEmptyReadNextBlock(sublistTwo, relationTwoTuples, relationTwo, blocksRead_tableTwo, mem, num_blocks_last_sublist_two, sublistOne.size());
//
//			// find the small tuple in each block in the memory
//			ArrayList<Tuple> smallTuples_relationOne = CommonHelper.smallestTuple(sublistOne, relationOneTuples, joinAttribute, null);
//			ArrayList<Tuple> smallTuples_relationTwo = CommonHelper.smallestTuple(sublistTwo, relationTwoTuples, joinAttribute, null);
//
//			// pick the smallest tuple of each relation
//			Tuple smallestTuple_relationOne =  Collections.min(smallTuples_relationOne,  new CompareTuplesMin(joinAttribute));
//			Tuple smallestTuple_relationTwo =  Collections.min(smallTuples_relationTwo,  new CompareTuplesMin(joinAttribute));
//
//			String commonFieldValue_relationOne = getFieldValue(smallestTuple_relationOne, joinAttribute);
//			String commonFieldValue_relationTwo = getFieldValue(smallestTuple_relationTwo, joinAttribute);
//			
//			if(commonFieldValue_relationOne != null && commonFieldValue_relationTwo != null) {
//				if(commonFieldValue_relationOne.equals(commonFieldValue_relationTwo)) {
//					// find the tuples having the same value of the join attribute as the smallest tuple value in all the sublists
//					ArrayList<Tuple> mathcingTuples_relationOne = getMatchingTuples(relationOneTuples, joinAttribute, commonFieldValue_relationOne);
//					ArrayList<Tuple> mathcingTuples_relationTwo = getMatchingTuples(relationTwoTuples, joinAttribute, commonFieldValue_relationTwo);
//					
//					// create a new relation for the output
//					Relation output_relation = CommonHelper.createRelation(relationOne, relationTwo, schema_manager, "NaturalJoin");
//					
//					//joinTuples and return the arraylist of all the resulting tuples
//					crossProductTuples(mathcingTuples_relationOne, mathcingTuples_relationTwo, output_relation, joinAttribute, result);
//					
//					// find the sublist of the smallest tuple
//					int sublist_num1 = smallTuples_relationOne.indexOf(smallestTuple_relationOne);
//					int sublist_num2 = smallTuples_relationTwo.indexOf(smallestTuple_relationTwo);
//					int smallest_index1 = relationOneTuples.get(sublist_num1).indexOf(smallestTuple_relationOne);
//					int smallest_index2 = relationTwoTuples.get(sublist_num2).indexOf(smallestTuple_relationTwo);
//					
//					Boolean keep_min_one = false;
//					Boolean keep_min_two = false;
//					
//					Block tempBlock;
//					for(int i = 0; i < sublistOne.size(); i++) {
//						tempBlock = mem.getBlock(i);
//						int size = relationOneTuples.get(i).size();
//						if((size-1 == smallest_index1)) {
//							keep_min_two = true;
//							break;
//						}
//					}
//					
//					for(int i = 0; i < sublistTwo.size(); i++) {
//						tempBlock = mem.getBlock(i+sublistOne.size());
//						int size = relationTwoTuples.get(i).size();
//						if((size-1 == smallest_index2)) {
//							keep_min_one = true;
//							break;
//						}
//					}
//					
//					
//					
//					if(keep_min_one && !keep_min_two) {
//						//relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
//						relationTwoTuples.get(sublist_num2).remove(smallest_index2);
//						//relationOneTuples.get(sublist_num1).remove(smallest_index1);
//					}
//					
//					if(!keep_min_one && keep_min_two) {
//						//relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
//						relationOneTuples.get(sublist_num1).remove(smallest_index1);
//						//relationTwoTuples.get(sublist_num2).remove(smallest_index2);
//					}
//					
//					
//					
//					if((keep_min_one && keep_min_two) || (!keep_min_one && !keep_min_two)) {
//						// delete both Relation one small tuples and relation two small tuples
//						//relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
//						//relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
//						relationOneTuples.get(sublist_num1).remove(smallest_index1);
//						relationTwoTuples.get(sublist_num2).remove(smallest_index2);
//					}
//				
//				}
//				else {					// values dont match, then delete the smallest one from the memory
//					if(CommonHelper.isStringInt(commonFieldValue_relationOne) && CommonHelper.isStringInt(commonFieldValue_relationTwo)) {								//field type is int
//						if(CommonHelper.stringToInteger(commonFieldValue_relationOne) - CommonHelper.stringToInteger(commonFieldValue_relationOne) > 0) {
//							relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
//						}
//						else {
//							relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
//						}
//					}
//					else {
//						if((commonFieldValue_relationOne.compareTo(commonFieldValue_relationTwo)) > 0) {
//							relationTwoTuples = deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
//						}
//						else {
//							relationOneTuples = deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
//						}
//					}
//				}
//			}
//
//////			System.out.println(relationOneTuples.size());
//////			System.out.println(relationTwoTuples.size());
//////			System.out.println("temp :" + temp);
////			temp = 0;
////			for(int i = 0; i < relationOneTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
////			//	if(relationOneTuples.get(i) != null) {
////					temp += relationOneTuples.get(i).size();
////			//	}
////			}
////			temp2 =0;
////			for(int i = 0; i < relationTwoTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
////			//	if(relationOneTuples.get(i) != null) { 
////					temp2 += relationTwoTuples.get(i).size();
////			//	}
////			}
////			
////			System.out.println("temp "+ temp );
////			System.out.println("temp2 "+ temp2 );
//			CommonHelper.ifEmptyReadNextBlock(sublistOne, relationOneTuples, relationOne, blocksRead_tableOne, mem, num_blocks_last_sublist_one, 0);
//			CommonHelper.ifEmptyReadNextBlock(sublistTwo, relationTwoTuples, relationTwo, blocksRead_tableTwo, mem, num_blocks_last_sublist_two, sublistOne.size());
//		}
//		return result;
//	}
//	
//	// return true if the arraylist of (arralist of tuples )is empty
//	public Boolean isListEmpty(ArrayList<ArrayList<Tuple>> list) {
//		for(ArrayList<Tuple> tuple :list) {
//			if(tuple.size() != 0) {
//				return false;
//			}
//		}
//		return true;
//	}
//	
//	// for a given tuple, return the value of a given field if tuple not null
//	public String getFieldValue(Tuple tuple, String fieldName) {
//		String fieldValue = null;
//		if(tuple != null) {
//			fieldValue = tuple.getField(fieldName).toString();
//		}
//		return fieldValue;
//	}
//	
//	// find the tuples having the same attribute value as the matching value
//	public ArrayList<Tuple> getMatchingTuples(ArrayList<ArrayList<Tuple>> relationtuples, String joinAttribute, String macthingValue){
//		System.out.println("Inside getMatchingTuples: matching value " +macthingValue );
//		System.out.println("Inside getMatchingTuples: join attribute " +joinAttribute );
//		ArrayList<Tuple> matchingTuples = new ArrayList<Tuple>();
//		// loop over each sublist
//		for(int i = 0; i < relationtuples.size(); i++) {
//			// loop over each block of tuples
//			for(int j = 0; j < relationtuples.get(i).size(); j++) {
//				if(relationtuples.get(i).get(j).getField(joinAttribute).toString().equals(macthingValue)) {
//					matchingTuples.add(relationtuples.get(i).get(j));
//				}
//			}
//			
//		}
//		return matchingTuples;
//	}
//	
//	public void crossProductTuples(ArrayList<Tuple> listOne, ArrayList<Tuple> listTwo, Relation relation, String joinAttribute, ArrayList<Tuple> result){
//		for(int i = 0; i < listOne.size(); i++) {
//			for(int j = 0; j < listTwo.size(); j++) {
//				result.add(CommonHelper.joinTuples(listOne.get(i), listTwo.get(j), relation, joinAttribute));
//			}
//		}
//
//	}
//	
////	public Relation createRelation(Relation relation_one, Relation relation_two, SchemaManager schema_manager) {
////		// field names and field types of relation one
////		ArrayList<String> field_names = relation_one.getSchema().getFieldNames();
////		ArrayList<String> field_names_two = relation_two.getSchema().getFieldNames();
////		
////		// field names and field types of the relation two
////		ArrayList<FieldType> field_types = relation_one.getSchema().getFieldTypes();
////		ArrayList<FieldType> field_types_two = relation_two.getSchema().getFieldTypes();
////		
////		// append all the field names of relation two to relation one field names
////		field_names.addAll(field_names_two);
////
////		// append all the field types of relation two to relation one field types
////		field_types.addAll(field_types_two);
////		
////		Schema schema = new Schema(field_names, field_types);
////		
////		String relation_name = relation_one.getRelationName() + "NaturalJoin" + relation_two.getRelationName();
////		if(schema_manager.relationExists(relation_name)) {
////			schema_manager.deleteRelation(relation_name);
////		}
////
////		Relation relation = schema_manager.createRelation(relation_name, schema);
////		return relation;
////	}
//	
//	
//	// delete the tuples from the listing that have the same field value as the matching value
//	public ArrayList<ArrayList<Tuple>> deleteSmallTuples(ArrayList<ArrayList<Tuple>> relationTuples, String matching_value, String field_name) {
////		System.out.print("matching_value : " + matching_value);
////		System.out.print("field_name : " + field_name);
//		for(int i = 0; i < relationTuples.size(); i++) {
//			for(int j = 0; j < relationTuples.get(i).size(); j++) {
//				if(relationTuples.get(i).get(j).getField(field_name).toString().equals(matching_value)) {
//					relationTuples.get(i).remove(relationTuples.get(i).get(j));
//				}
//			}
//		}
//		return relationTuples;
//	}
//	
//	public static Boolean isSublistEmpty(ArrayList<ArrayList<Tuple>> relationTuples) {
//		for(ArrayList<Tuple> list : relationTuples) {
//			if(list.size() != 0) {
//				return false;
//			}
//		}
//		return true;
//	}
//	
//	// get the same value tuples in a single segment
//	public static int getSameTuplesCountSingleSegment(String matchingValue, String fieldName, ArrayList<Tuple> relationTuples) {
//		int count = 0;
//		for(Tuple tuple : relationTuples) {
//			if(tuple.getField(fieldName).toString().equals(matchingValue)) {
//				count++;
//			}
//		}
//		return count;
//	}
//	
//	// get the same value tuples in all the segments of a relation
//	public static int getSameTuplesCountAllSegments(String matching_value, String field_name, ArrayList<ArrayList<Tuple>> relationTuples) {
//		int count = 0;
//		for(int i = 0; i < relationTuples.size(); i++) {
//			for(int j = 0; j < relationTuples.get(i).size(); j++) {
//				if(relationTuples.get(i).get(j).getField(field_name).toString().equals(matching_value)) {
//					relationTuples.get(i).remove(relationTuples.get(i).get(j));
//				}
//			}
//		}
//		return count;
//	}
//	
//	
//}