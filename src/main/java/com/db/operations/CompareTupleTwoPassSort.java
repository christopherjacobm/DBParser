package com.db.operations;

import java.util.ArrayList;
import java.util.Comparator;

import com.db.storageManager.Tuple;

public class CompareTupleTwoPassSort implements Comparator<Tuple> {

ArrayList<String> fieldName;
	
	public CompareTupleTwoPassSort(ArrayList<String> fieldName) {
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
			
			String valueOne = tupleOne.getField(fieldName.get(0)).toString();
			String valueTwo = tupleOne.getField(fieldName.get(0)).toString();
			// if the field is an int
			if(CommonHelper.isStringInt(valueOne) && CommonHelper.isStringInt(valueTwo)) {
				return CommonHelper.stringToInteger(valueOne)- CommonHelper.stringToInteger(valueTwo);
			}
			else {
				return valueOne.compareTo(valueTwo);
			}
		}
}
