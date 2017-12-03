package com.db.operations;
import java.util.ArrayList;
import java.util.Comparator;

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
		for(int i = 0; i < sortByAttributes.size(); i++) {
			fieldOne_value = tupleOne.getField(sortByAttributes.get(i)).toString();		// get field value from first tuple
			fieldTwo_value = tupleTwo.getField(sortByAttributes.get(i)).toString();		// get field value from second tuple
			// check if the values are integer
			if(isStringInt(fieldOne_value) && isStringInt(fieldTwo_value)) {
				output[i] = stringToInteger(fieldOne_value) - stringToInteger(fieldTwo_value);
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
}
