package com.db.operations;

import java.util.ArrayList;
import java.util.Collections;

import com.db.storageManager.FieldType;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.Schema;
import com.db.storageManager.SchemaManager;
import com.db.storageManager.Tuple;

public class CommonHelper {

	public static Relation createRelation(Relation relation_one, Relation relation_two, SchemaManager schema_manager, String operationName) {
		// field names and field types of relation one
		ArrayList<String> field_names = relation_one.getSchema().getFieldNames();
		ArrayList<String> field_names_two = relation_two.getSchema().getFieldNames();
		
		// field names and field types of the relation two
		ArrayList<FieldType> field_types = relation_one.getSchema().getFieldTypes();
		ArrayList<FieldType> field_types_two = relation_two.getSchema().getFieldTypes();

		/*for(int i=0;i<field_names.size();i++){
			if (field_names_two.contains(field_names.get(i))) {//remove the common field name (assuming only one is common)
				field_names.remove(i);
				field_types.remove(i);
				break;
			}
		} */
		//todo uncomment for nat join

		for(int i=0;i<field_names.size();i++) {
			String colName = field_names.get(i);
			if (field_names_two.contains(colName)) {//remove the common field name (assuming only one is common)
				//rename both to tablename+attrname
				field_names.set(i, relation_one.getRelationName() + '.' + colName);
				field_names_two.set(field_names_two.indexOf(colName), relation_two.getRelationName() + '.' + colName);
			}
		}
		
		// append all the field names of relation two to relation one field names
		field_names.addAll(field_names_two);

		// append all the field types of relation two to relation one field types
		field_types.addAll(field_types_two);
		
		Schema schema = new Schema(field_names, field_types);
		
		String relation_name = relation_one.getRelationName() + operationName + relation_two.getRelationName();
		if(schema_manager.relationExists(relation_name)) {
			schema_manager.deleteRelation(relation_name);
		}

		Relation relation = schema_manager.createRelation(relation_name, schema);
		//System.out.println("in createRelation: field names- "+schema.getFieldNames());
		return relation;
	}
	
	// return true if the value is int
	public static Boolean isStringInt(String value) {
		try {
			Integer.parseInt(value);
			return true;
		}
		catch(NumberFormatException ex){
			return false;
		}
	}
	
	// convert a value from string to int
	public static int stringToInteger(String value) {
		return Integer.parseInt(value);
		
	}
	
	// join two tuples into one new tuple
	public static Tuple joinTuples(Tuple tupleOne, Tuple tupleTwo, Relation relation, String joinAttribute) {

		String setValue = null;
		Tuple joinedTuple = relation.createTuple();
		int tuple_one_size = tupleOne.getNumOfFields();
		int tuple_two_size = tupleTwo.getNumOfFields();
		int newTuple_size = tuple_one_size + tuple_two_size;
		int setIndex=0;
		for(int i=0;i < newTuple_size;i++) {
			if(i < tuple_one_size) {
				if (joinAttribute!=null && tupleOne.getSchema().getFieldOffset(joinAttribute)==i) { //if current field's name == joinAttribute, continue;
					continue;
				}
				setValue = tupleOne.getField(i).toString();
			}
			else {
				setValue = tupleTwo.getField(i - tuple_one_size).toString();
			}

			if(CommonHelper.isStringInt(setValue)) {								// field is of type int
				joinedTuple.setField(setIndex++, CommonHelper.stringToInteger(setValue));
			}
			else {																			// field is of type string
				joinedTuple.setField(setIndex++, setValue);
			}
		}
		//System.out.println("in joinTuples: joinedTuple field names "+joinedTuple.getSchema().getFieldNames());
		return joinedTuple;
	}

	// get the number starting block number of all the sublists
	public static ArrayList<Integer> getSublist(Relation relation, MainMemory mem){
		
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
	private static ArrayList<Integer> getSubList(ArrayList<Integer> subList , int relation_size, int mem_size) {
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
	public static int getLastSublistBlocksCount(Relation relation, MainMemory mem) {
		int num_blocks = 0;
		
		if(relation.getNumOfBlocks() <= mem.getMemorySize()) {
			num_blocks = relation.getNumOfBlocks();
		}else {
			num_blocks = relation.getNumOfBlocks() % 10;
		}
		return num_blocks;
	}
	
	public static void phaseOne(MainMemory mem, Relation relation, ArrayList<String> sortByAttributes) {
		
		// number of blocks being processed at a time
		int num_blocks = 0;
		
		// a variable to keep track of the number of sorted blocks at any time
		int sortedBlocks = 0;
		
		// total number of blocks of the given relation
		int relationBlocks = relation.getNumOfBlocks();
		
		// size of main memory (10)
		int mem_size = mem.getMemorySize();
		

		
		// while all the blocks on disk are not sorted
		while(sortedBlocks < relationBlocks) {
			int temp = relationBlocks - sortedBlocks;
			if(temp > mem_size) {				// blocks left to be processed dont fit in the main memory
				num_blocks= mem_size;
			}
			else {								// blocks left to be processed fit in the main memory
				num_blocks = temp;
			}
			// read severals blocks from the disk and store in the main memory
			relation.getBlocks(sortedBlocks, 0, num_blocks);

			System.out.println("inside the while loop********************************************8f");
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
	
	public static MainMemory clearMem(MainMemory mem) {
		// clean out the main memory
		for(int i = 0; i < mem.getMemorySize(); i++) {
			mem.getBlock(i).clear();
		}
		return mem;
	}

	
	public static ArrayList<ArrayList<Tuple>> readBlockFromSublist(ArrayList<Integer> sublist, Relation relation, MainMemory mem, ArrayList<ArrayList<Tuple>> relationTuples, int startingIndex, int[] blocksRead_tableOne) {
		System.out.print("Inside readBlockFromSublist : sublist_size" + sublist.size());
		// read one block from each sublist in relation One into the main memory
		for(int i = 0; i < sublist.size(); i++) {
			relation.getBlock(sublist.get(i), i + startingIndex);            			// read one block from relation and store it in the main memory at the given index
			relationTuples.add(mem.getBlock(i+startingIndex).getTuples());			//  read block from main memory stored at the given index																					//  store all the tuples in the given block in an array list 
			blocksRead_tableOne[i]++;
		}
		return relationTuples;
	}
	
	public static void ifEmptyReadNextBlock(ArrayList<Integer> sublist, ArrayList<ArrayList<Tuple>> relationTuples, Relation relation, int[] blocksRead, MainMemory mem, int num_blocks_last_sublist, int startingIndex) {
		int last_element = sublist.size() -1;
		// if the blocks in memory are empty, bring in the next block from disk
		for(int i =0 ; i < sublist.size(); i++) {
			if(relationTuples.get(i).isEmpty()) {
				// not last element and there are more blocks to read from the sublist
				if(i < last_element && blocksRead[i] < mem.getMemorySize()) {							// if not the last element in the arraylist, that last sublist does not gurantee to have 10 bloacks
					relation.getBlock(sublist.get(i) + blocksRead[i], i + startingIndex);
					relationTuples.get(i).addAll(mem.getBlock(i + startingIndex).getTuples());									    // add all the tuples read into memory into the arraylist
					blocksRead[i]++;       															// increment the number of read blocks of the sublist
				}
				else if(i == last_element && blocksRead[i] < num_blocks_last_sublist) {     		// last sublist and all the blocks in sublist have not been read yet
					relation.getBlock(sublist.get(i) + blocksRead[i], i + startingIndex);
					relationTuples.get(i).addAll(mem.getBlock(i + startingIndex).getTuples());									    // add all the tuples read into memory into the arraylist
					blocksRead[i]++; 																// increment the number of read blocks of the sublist
				}
			}
		}
	}
	
	// pick smallest tuple from each block of a relation
	// sortByAttributes is null for two pass natural join
	// fieldName is null for two pass sorting
	public static ArrayList<Tuple> smallestTuple(ArrayList<Integer> sublist, ArrayList<ArrayList<Tuple>> relationTuples, String fieldName, ArrayList<String> sortByAttributes) {
		// array storing the smallest tuple from each block
		ArrayList<Tuple> smallestTuples = new ArrayList<Tuple>();
		for(int i = 0; i < sublist.size(); i++) {
			if(relationTuples.get(i).size() != 0) {
				// find the smallest tuple by comparing tuples
				if(fieldName != null) {
					smallestTuples.add(Collections.min(relationTuples.get(i),  new CompareTuplesMin(fieldName)));
				}
				else {
					smallestTuples.add(Collections.min(relationTuples.get(i),  new CompareTuplesSort(sortByAttributes)));
				}
			}
			else {
				smallestTuples.add(null);			// block is empty
			}
		}
		return smallestTuples;
	}
}
