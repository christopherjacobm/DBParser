package com.db.operations;

import java.util.ArrayList;

import com.db.storageManager.FieldType;
import com.db.storageManager.Relation;
import com.db.storageManager.Schema;
import com.db.storageManager.SchemaManager;
import com.db.storageManager.Tuple;

public class CommonHelper {

	public static Relation createRelation(Relation relation_one, Relation relation_two, SchemaManager schema_manager) {
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
	public static Tuple joinTuples(Tuple tupleOne, Tuple tupleTwo, Relation relation) {
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

			if(CommonHelper.isStringInt(setValue)) {								// field is of type int
				joinedTuple.setField(i, CommonHelper.stringToInteger(setValue));
			}
			else {																			// field is of type string
				joinedTuple.setField(i, setValue);
			}
		}
		return joinedTuple;
	}
}
