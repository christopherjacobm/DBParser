package com.db.operations;
import java.util.ArrayList;
import java.util.Comparator;

import com.db.parser.Helper;
import com.db.storageManager.Tuple;

public class CompareTuplesSort implements Comparator<Tuple>{
	// comapre two tuples based on the field values
	String fieldOne_value;
	String fieldTwo_value;	
	ArrayList<String> sortByAttributes = null;
	int[] output;
	
	public CompareTuplesSort(ArrayList<String> sortByAttributes) {
		this.sortByAttributes = sortByAttributes;
		this.output = new int[sortByAttributes.size()];
	}
	@Override
	public int compare(Tuple tupleOne, Tuple tupleTwo) {
		if(tupleOne == null) {
			return 1;
		}else if(tupleTwo == null){
			return -1;
		}
		for(int i = 0; i < sortByAttributes.size(); i++) {
			fieldOne_value = tupleOne.getField(Helper.getColNameMatchingToken(sortByAttributes.get(i),tupleOne)).toString();		// get field value from first tuple
			fieldTwo_value = tupleTwo.getField(Helper.getColNameMatchingToken(sortByAttributes.get(i),tupleTwo)).toString();		// get field value from second tuple
			// check if the values are integer
			if(CommonHelper.isStringInt(fieldOne_value) && CommonHelper.isStringInt(fieldTwo_value)) {
				output[i] = CommonHelper.stringToInteger(fieldOne_value) - CommonHelper.stringToInteger(fieldTwo_value);
			}else {
				output[i] = fieldOne_value.compareTo(fieldTwo_value);
			}			
		}
		// return -1 if tupleOne < tupleTwo
		// return 1 if tupleOne > tupleTwo
		for(int i = 0; i < output.length; i++) {
			if(output[i] < 0) {
				return -1;
			}else if(output[i] > 0) {	                  
				return 1;
			}
		}
		// return 0 when tupleOne = tupleTwo ie tuples have same field values
		return 0;
	}	
}
