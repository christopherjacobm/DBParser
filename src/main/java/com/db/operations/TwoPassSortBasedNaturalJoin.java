package com.db.operations;

import java.util.ArrayList;
import java.util.Collections;
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

		//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee6e");
		// clean out the main memory
		mem = CommonHelper.clearMem(mem);
		//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee7");
		// get the starting blocks number of each sublist as an array
		ArrayList<Integer> sublistOne = CommonHelper.getSublist(relationOne, mem);

		//System.out.println();
		ArrayList<Integer> sublistTwo = CommonHelper.getSublist(relationTwo, mem);




		//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee87");
		// number of blocks in the last sublist, in other sublists there are exactly 10 blocks
		int num_blocks_last_sublist_one = CommonHelper.getLastSublistBlocksCount(relationOne, mem);
		int num_blocks_last_sublist_two = CommonHelper.getLastSublistBlocksCount(relationTwo, mem);
		//System.out.println("num_blocks_last_sublist_two: " + num_blocks_last_sublist_one);
		//System.out.println("num_blocks_last_sublist_two : "+ num_blocks_last_sublist_two);
		//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee9");
		// to store the number of read blocks in each sublist
		int[] blocksRead_tableOne = new int[sublistOne.size()];
		int[] blocksRead_tableTwo = new int[sublistTwo.size()];
		//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee10");
		// read one block from each sublist in each relation into the main memory and store tuples 
		relationOneTuples = CommonHelper.readBlockFromSublist(sublistOne, relationOne, mem, relationOneTuples, 0, blocksRead_tableOne);         					// startingIndex = 0, memory blocks empty
		relationTwoTuples = CommonHelper.readBlockFromSublist(sublistTwo, relationTwo, mem, relationTwoTuples, sublistOne.size(), blocksRead_tableTwo);		    // startingIndex = sublistOne.size(), one block from each sublist of relation one already exist in the memory

		int temp =0;
		for(int i = 0; i < relationOneTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
			 temp += relationOneTuples.get(i).size();
		}

		// result of natural join of two tables
		ArrayList<Tuple> result = null;
		while(temp>0) {


			// if the blocks in memory are empty, bring in the next block from disk
			CommonHelper.ifEmptyReadNextBlock(sublistOne, relationOneTuples, relationOne, blocksRead_tableOne, mem, num_blocks_last_sublist_one, 0);
			CommonHelper.ifEmptyReadNextBlock(sublistTwo, relationTwoTuples, relationTwo, blocksRead_tableTwo, mem, num_blocks_last_sublist_two, sublistOne.size());
			//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee4");
			// find the small tuple in each block in the memory
			ArrayList<Tuple> smallTuples_relationOne = CommonHelper.smallestTuple(sublistOne, relationOneTuples, joinAttribute, null);
			ArrayList<Tuple> smallTuples_relationTwo = CommonHelper.smallestTuple(sublistTwo, relationTwoTuples, joinAttribute, null);
			//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee3");
			// pick the smallest tuple of each relation
			Tuple smallestTuple_relationOne =  Collections.min(smallTuples_relationOne,  new CompareTuplesMin(joinAttribute));
			Tuple smallestTuple_relationTwo =  Collections.min(smallTuples_relationTwo,  new CompareTuplesMin(joinAttribute));
			//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee2e");
			String commonFieldValue_relationOne = getFieldValue(smallestTuple_relationOne, joinAttribute);
			String commonFieldValue_relationTwo = getFieldValue(smallestTuple_relationTwo, joinAttribute);
			//System.out.println("Herreeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee2e");
			
			if(commonFieldValue_relationOne != null && commonFieldValue_relationTwo != null) {
				if(commonFieldValue_relationOne.equals(commonFieldValue_relationTwo)) {
					// find the tuples having the same value of the join attribute as the smallest tuple value in all the sublists
					ArrayList<Tuple> mathcingTuples_relationOne = getMatchingTuples(relationOneTuples, joinAttribute, commonFieldValue_relationOne);
					ArrayList<Tuple> mathcingTuples_relationTwo = getMatchingTuples(relationTwoTuples, joinAttribute, commonFieldValue_relationTwo);
					
					// create a new relation for the output
					Relation output_relation = CommonHelper.createRelation(relationOne, relationTwo, schema_manager);
					
					//joinTuples and return the arraylist of all the resulting tuples
					result = crossProductTuples(mathcingTuples_relationOne, mathcingTuples_relationTwo, output_relation, joinAttribute);
					
					// delete both Relation one small tuples and relation two small tuples
					deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
					deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
			
				}
			}else {					// values dont match, then delete the smallest one from the memory
				if(CommonHelper.isStringInt(commonFieldValue_relationOne) && CommonHelper.isStringInt(commonFieldValue_relationTwo)) {								//field type is int
					if(CommonHelper.stringToInteger(commonFieldValue_relationOne) - CommonHelper.stringToInteger(commonFieldValue_relationOne) > 0) {
						deleteSmallTuples(relationTwoTuples, commonFieldValue_relationOne, joinAttribute);
					}
					else {
						deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
					}
				}
				else {
					if((commonFieldValue_relationOne.compareTo(commonFieldValue_relationTwo)) > 0) {
						deleteSmallTuples(relationTwoTuples, commonFieldValue_relationOne, joinAttribute);
					}
					else {
						deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
					}
				}
			}
			System.out.println(relationOneTuples.size());
			System.out.println(relationTwoTuples.size());
			System.out.println("temp :" + temp);

			for(int i = 0; i < relationOneTuples.size(); i++){																																									// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
				temp += relationOneTuples.get(i).size();
			}
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
		System.out.println("Inside getMatchingTuples: matching value " +macthingValue );
		System.out.println("Inside getMatchingTuples: join attribute " +joinAttribute );
		ArrayList<Tuple> matchingTuples = new ArrayList<Tuple>();
		// loop over each sublist
		for(int i = 0; i < relationtuples.size(); i++) {
			// loop over each block of tuples
			for(int j = 0; j < relationtuples.get(i).size(); j++) {
				if(relationtuples.get(i).get(j).getField(joinAttribute).toString().equals("Chris")) {
					matchingTuples.add(relationtuples.get(i).get(j));
				}
			}
			
		}
		return matchingTuples;
	}
	
	public ArrayList<Tuple> crossProductTuples(ArrayList<Tuple> listOne, ArrayList<Tuple> listTwo, Relation relation, String joinAttribute){
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for(int i = 0; i < listOne.size(); i++) {
			for(int j = 0; i < listTwo.size(); i++) {
				result.add(CommonHelper.joinTuples(listOne.get(i), listOne.get(j), relation, joinAttribute));
			}
		}
		return result;
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
	public void deleteSmallTuples(ArrayList<ArrayList<Tuple>> relationTuples, String matching_value, String field_name) {
		System.out.print("matching_value : " + matching_value);
		System.out.print("field_name : " + field_name);
		for(int i = 0; i < relationTuples.size(); i++) {
			for(int j = 0; j < relationTuples.get(i).size(); j++) {
				if(relationTuples.get(i).get(j).getField(field_name).toString().equals(matching_value)) {
					System.out.print("inside the if statement, matching_value : " + matching_value);
					System.out.print("inside the if statement, field_name : " + field_name);
					relationTuples.get(i).remove(relationTuples.get(i).get(j));
				}
			}
		}
		
	}
	
	
}