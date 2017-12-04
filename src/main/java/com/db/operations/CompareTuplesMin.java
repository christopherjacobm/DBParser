package com.db.operations;

import java.util.Comparator;

import com.db.storageManager.Tuple;

public class CompareTuplesMin implements Comparator<Tuple> {
	
	String fieldName;
	
	public CompareTuplesMin(String fieldName) {
		this.fieldName = fieldName;
	}
		@Override
		public int compare(Tuple tupleOne, Tuple tupleTwo) {
			if(tupleOne == null) {
				return 1;
			}
			if(tupleTwo == null) {
				return -1;
			}
			
			String valueOne = tupleOne.getField(fieldName).toString();
			String valueTwo = tupleOne.getField(fieldName).toString();
			// if the field is an int
			if(CommonHelper.isStringInt(valueOne) && CommonHelper.isStringInt(valueTwo)) {
				return CommonHelper.stringToInteger(valueOne)- CommonHelper.stringToInteger(valueTwo);
			}
			else {
				return valueOne.compareTo(valueTwo);
			}
		}
}