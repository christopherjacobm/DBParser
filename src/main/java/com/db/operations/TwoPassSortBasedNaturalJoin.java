package com.db.operations;

import java.util.ArrayList;
import java.util.Collections;
import com.db.storageManager.FieldType;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Schema;
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
		
		this.phaseOne(mem, relationOne, sortBy);
		this.phaseOne(mem, relationTwo, sortBy);
		
		// clean out the main memory
		for(int i = 0; i < mem.getMemorySize(); i++) {
			mem.getBlock(i).clear();
		}
		
		// get the starting blocks number of each sublist as an array
		ArrayList<Integer> sublistOne = getSublist(relationOne, mem);
		ArrayList<Integer> sublistTwo = getSublist(relationTwo, mem);
		
		// number of blocks in the last sublist, in other sublists there are exactly 10 blocks
		int num_blocks_last_sublist_one = getLastSublistBlocksCount(relationOne, mem);
		int num_blocks_last_sublist_two = getLastSublistBlocksCount(relationTwo, mem);
		
		// read one block from each sublist in each relation into the main memory and store tuples 
		readBlockFromSublist(sublistOne, relationOne, mem, relationOneTuples, 0);         					// startingIndex = 0, memory blocks empty
		readBlockFromSublist(sublistTwo, relationTwo, mem, relationTwoTuples, sublistOne.size());		    // startingIndex = sublistOne.size(), one block from each sublist of relation one already exist in the memory
																															// append the blosks of 2nd relation below the bloacks of the 1st relation in the memory
		// to store the number of read blocks in each sublist
		int[] blocksRead_tableOne = new int[sublistOne.size()];
		int[] blocksRead_tableTwo = new int[sublistTwo.size()];
		
		// initialize all the values to be 1 since one block of each sublist has been read into the main memory
		setInitialValue(blocksRead_tableOne);
		setInitialValue(blocksRead_tableTwo);
		
		// result of natural join of two tables
		ArrayList<Tuple> result = null;
		
		while(isListEmpty(relationOneTuples) && isListEmpty(relationTwoTuples)) {
			// if the blocks in memory are empty, bring in the next block from disk
			ifEmptyReadNextBlock(sublistOne, relationOneTuples, relationOne, blocksRead_tableOne, mem, num_blocks_last_sublist_one, 0);
			ifEmptyReadNextBlock(sublistTwo, relationTwoTuples, relationTwo, blocksRead_tableTwo, mem, num_blocks_last_sublist_two, sublistOne.size());
			
			// find the small tuple in each block in the memory
			ArrayList<Tuple> smallTuples_relationOne = smallestTuple(sublistOne, relationOneTuples, joinAttribute);
			ArrayList<Tuple> smallTuples_relationTwo = smallestTuple(sublistTwo, relationTwoTuples, joinAttribute);
			
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
					Relation output_relation = createRelation(relationOne, relationTwo, schema_manager);
					
					//joinTuples and return the arraylist of all the resulting tuples
					result = crossProductTuples(mathcingTuples_relationOne, mathcingTuples_relationTwo, output_relation);
					
					// delete both Relation one small tuples and relation two small tuples
					deleteSmallTuples(relationOneTuples, commonFieldValue_relationOne, joinAttribute);
					deleteSmallTuples(relationTwoTuples, commonFieldValue_relationTwo, joinAttribute);
			
				}
			}else {					// values dont match, then delete the smallest one from the memory
				if(isStringInt(commonFieldValue_relationOne) && isStringInt(commonFieldValue_relationTwo)) {								//field type is int
					if(stringToInteger(commonFieldValue_relationOne) - stringToInteger(commonFieldValue_relationOne) > 0) {
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
		}
		return result;
	}
	
	
	public void phaseOne(MainMemory mem, Relation relation, ArrayList<String> sortByAttributes) {
		
		// number of blocks being processed at a time
		int num_blocks = 0;
		
		// a variable to keep track of the number of sorted blocks at any time
		int sortedBlocks = 0;
		
		// total number of blocks of the given relation
		int relationBlocks = relation.getNumOfBlocks();
		
		// size of main memory (10)
		int mem_size = mem.getMemorySize();
		
		int temp = relationBlocks - sortedBlocks;
		
		// while all the blocks on disk are not sorted
		while(sortedBlocks != relationBlocks) {
			if(temp > mem_size) {				// blocks left to be processed dont fit in the main memory
				num_blocks= mem_size;
			}
			else {								// blocks left to be processed fit in the main memory
				num_blocks = temp;
			}
			// read severals blocks from the disk and store in the main memory
			relation.getBlocks(sortedBlocks, 0, num_blocks);
			
			// read all the tuples stored in the main memory starting from block index 0 to num_blocks
			ArrayList<Tuple> tuples = mem.getTuples(0, num_blocks);
			
			// sort the tuples and make them sorted sublists
			Collections.sort(tuples, new CompareTuplesSort(sortByAttributes));
			
			// write sorted sublists to main memory
			mem.setTuples(0, tuples);
			
			// read sorted sublists from the main memory and store on the disk
			relation.setBlocks(sortedBlocks, 0, num_blocks);
			
			// update the number of sorted bloacks in the memory
			sortedBlocks = sortedBlocks + num_blocks;			
		}
	}
	
	// comapre two tuples based on the field values
//	class CompareTuples implements Comparator<Tuple>{
//		String fieldOne_value;
//		String fieldTwo_value;	
//		ArrayList<String> sortByAttributes = null;
//		int[] output;
//		
//		public CompareTuples(ArrayList<String> sortByAttributes) {
//			this.sortByAttributes = sortByAttributes;
//			this.output = new int[sortByAttributes.size()];
//		}
//		@Override
//		public int compare(Tuple tupleOne, Tuple tupleTwo) {
//			for(int i = 0; i < sortByAttributes.size(); i++) {
//				fieldOne_value = tupleOne.getField(sortByAttributes.get(i)).toString();		// get field value from first tuple
//				fieldTwo_value = tupleTwo.getField(sortByAttributes.get(i)).toString();		// get field value from second tuple
//				// check if the values are integer
//				if(isStringInt(fieldOne_value) && isStringInt(fieldTwo_value)) {
//					output[i] = stringToInteger(fieldOne_value) - stringToInteger(fieldTwo_value);
//				}else {
//					output[i] = fieldOne_value.compareTo(fieldTwo_value);
//				}			
//			}
//			// return -1 if tupleOne < tupleTwo
//			// return 1 if tupleOne > tupleTwo
//			for(int i = 0; i < output.length; i++) {
//				if(output[i] < 0) {
//					return -1;
//				}else if(output[i] > 0) {	                  
//					return 1;
//				}
//			}
//			// return 0 when tupleOne = tupleTwo ie tuples have same field values
//			return 0;
//		}			
//	}
	
	// return true if the value is int
	public Boolean isStringInt(String value) {
		try {
			Integer.parseInt(value);
			return true;
		}
		catch(NumberFormatException ex){
			return false;
		}
	}
	
	// convert a value from string to int
	public int stringToInteger(String value) {
		return Integer.parseInt(value);
		
	}
	
	
	// get the number starting block number of all the sublists
	public ArrayList<Integer> getSublist(Relation relation, MainMemory mem){
		
		// array to store the starting block number of each sublists
		ArrayList<Integer> sublist_relation_one = new ArrayList<Integer>(); 
		
		// first sublists always start at block 0
		sublist_relation_one.add(0);
		
		// size of a relation
		int relation_size = relation.getNumOfBlocks();
		
		// memory size
		int mem_size = mem.getMemorySize();
		return getSubList(sublist_relation_one, relation_size, mem_size);
	}
	
	// helper method to get the block number of different sublists recursively
	// eg: if relation size 48,  sublist_relation_one = < 0, 10, 20, 30, 40>
	private ArrayList<Integer> getSubList(ArrayList<Integer> subList , int relation_size, int mem_size) {
		int value;
		if(relation_size > mem_size) {
			value = relation_size / mem_size;
			relation_size = relation_size - mem_size;
			getSubList(subList, relation_size, mem_size);
			subList.add(value * 10);
		}
		return subList;
	}
	
	// number of blocks in the last sublist, in other sublists there are exactly 10 blocks
	public int getLastSublistBlocksCount(Relation relation, MainMemory mem) {
		int num_blocks = 0;
		
		if(relation.getNumOfBlocks() <= mem.getMemorySize()) {
			num_blocks = relation.getNumOfBlocks();
		}else {
			num_blocks = relation.getNumOfBlocks() % 10;
		}
		return num_blocks;
	}
	
	public void setInitialValue(int[] array) {
		// set the initial value 
		for(int i = 0; i < array.length; i++) {
			array[i] = 1;
		}
	}
	
	public void readBlockFromSublist(ArrayList<Integer> sublist, Relation relation, MainMemory mem, ArrayList<ArrayList<Tuple>> relationTuples, int startingIndex) {

		// read one block from each sublist in relation One into the main memory
		for(int i = 0; i < sublist.size(); i++) {
			relation.getBlock(sublist.get(i), i + startingIndex);            			// read one block from relation and store it in the main memory at the given index
			relationTuples.add(mem.getBlock(i+startingIndex).getTuples());			//  read block from main memory stored at the given index
																					//  store all the tuples in the given block in an array list 
		}	
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
	
	public void ifEmptyReadNextBlock(ArrayList<Integer> sublist, ArrayList<ArrayList<Tuple>> relationTuples, Relation relation, int[] blocksRead, MainMemory mem, int num_blocks_last_sublist, int startingIndex) {
		int last_element = sublist.size() -1;
		// if the blocks in memory are empty, bring in the next block from disk
		for(int i =0 ; i < sublist.size(); i++) {
			if(relationTuples.get(i).size() == 0) {
				// not last element and there are more blocks to read from the sublist
				if(i < last_element && blocksRead[i] < mem.getMemorySize()) {							// if not the last element in the arraylist, that last sublist does not gurantee to have 10 bloacks
					relation.getBlock(sublist.get(i) + blocksRead[i], i + startingIndex);
					relationTuples.add(mem.getBlock(i + startingIndex).getTuples());									    // add all the tuples read into memory into the arraylist
					blocksRead[i]++;       															// increment the number of read blocks of the sublist
				}
				else if(i == last_element && blocksRead[i] < num_blocks_last_sublist) {     		// last sublist and all the blocks in sublist have not been read yet
					relation.getBlock(sublist.get(i) + blocksRead[i], i + startingIndex);
					relationTuples.add(mem.getBlock(i + startingIndex).getTuples());									    // add all the tuples read into memory into the arraylist
					blocksRead[i]++; 																// increment the number of read blocks of the sublist
				}
			}
		}
	}
	
	// pick smallest tuple from each block of a relation
	public ArrayList<Tuple> smallestTuple(ArrayList<Integer> sublist, ArrayList<ArrayList<Tuple>> relationTuples, String fieldName) {
		// array storing the smallest tuple from each block
		ArrayList<Tuple> smallestTuples = new ArrayList<Tuple>();
		for(int i = 0; i < sublist.size(); i++) {
			if(relationTuples.get(i).size() != 0) {
				// find the smallest tuple by comparing tuples
				smallestTuples.add(0,Collections.min(relationTuples.get(i),  new CompareTuplesMin(fieldName)));
			}
			else {
				smallestTuples.addAll(0, null);			// block is empty
			}
		}
		return smallestTuples;
	}
	
	// for a given tuple, return the value of a given field if tuple not null
	public String getFieldValue(Tuple tuple, String fieldName) {
		String fieldValue = null;
		if(tuple != null) {
			fieldValue = tuple.getField(fieldName).toString();
		}
		return fieldName;
	}
	
	// find the tuples having the same attribute value as the matching value
	public ArrayList<Tuple> getMatchingTuples(ArrayList<ArrayList<Tuple>> relationtuples, String joinAttribute, String macthingValue){
		ArrayList<Tuple> matchingTuples = new ArrayList<Tuple>();
		// loop over each sublist
		for(int i = 0; i < relationtuples.size(); i++) {
			// loop over each block of tuples
			for(int j = 0; j < relationtuples.get(i).size(); i++) {
				if(relationtuples.get(i).get(j).getField(joinAttribute).toString().equals(macthingValue)) {
					matchingTuples.add(relationtuples.get(i).get(j));
				}
			}
			
		}
		return matchingTuples;
	}
	
	// join two tuples into one new tuple
	public Tuple joinTuples(Tuple tupleOne, Tuple tupleTwo, Relation relation) {
		String setValue = null;
		Tuple joinedTuple = relation.createTuple();
		int tuple_one_size = tupleOne.getNumOfFields();
		int tuple_two_size = tupleTwo.getNumOfFields();
		int newTuple_size = tuple_one_size + tuple_two_size;
		for(int i = 0; i < newTuple_size; i++) {
			if(i < tuple_one_size) {
				setValue = tupleOne.getField(i).toString();
			}
				else {
					setValue = tupleOne.getField(i - tuple_one_size).toString();
			}

			if(isStringInt(setValue)) {								// field is of type int
				joinedTuple.setField(i, stringToInteger(setValue));
			}
			else {																			// field is of type string
				joinedTuple.setField(i, setValue);
			}
		}
		return joinedTuple;
	}
	
	public ArrayList<Tuple> crossProductTuples(ArrayList<Tuple> listOne, ArrayList<Tuple> listTwo, Relation relation){
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for(int i = 0; i < listOne.size(); i++) {
			for(int j = 0; i < listTwo.size(); i++) {
				result.add(joinTuples(listOne.get(i), listOne.get(j), relation));
			}
		}
		return result;
	}
	
	public Relation createRelation(Relation relation_one, Relation relation_two, SchemaManager schema_manager) {
		// field names and field types of relation one
		ArrayList<String> field_names = relation_one.getSchema().getFieldNames();
		ArrayList<String> field_names_two = relation_two.getSchema().getFieldNames();
		
		// field names and field types of the relation two
		ArrayList<FieldType> field_types = relation_one.getSchema().getFieldTypes();
		ArrayList<FieldType> field_types_two = relation_two.getSchema().getFieldTypes();
		
		// append all the field names of relation two to relation one field names
		field_names.addAll(field_names_two);

		// append all the field types of relation two to relation one field types
		field_types.addAll(field_types_two);
		
		Schema schema = new Schema(field_names, field_types);
		
		String relation_name = relation_one.getRelationName() + "NaturalJoin" + relation_two.getRelationName();
		if(schema_manager.relationExists(relation_name)) {
			schema_manager.deleteRelation(relation_name);
		}

		Relation relation = schema_manager.createRelation(relation_name, schema);
		return relation;
	}
	
	
	// delete the tuples from the listing that have the same field value as the matching value
	public void deleteSmallTuples(ArrayList<ArrayList<Tuple>> relationTuples, String matching_value, String field_name) {
		for(int i = 0; i < relationTuples.size(); i++) {
			for(int j = 0; i < relationTuples.get(i).size(); i++) {
				if(relationTuples.get(i).get(j).getField(field_name).toString().equals(matching_value)) {
					relationTuples.get(i).remove(relationTuples.get(i).get(j));
				}
			}
		}
		
	}
	
	
}
