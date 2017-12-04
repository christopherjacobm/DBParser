package com.db.operations;

import java.util.ArrayList;
import java.util.Arrays;

import com.db.parser.InsertStatement;
import com.db.parser.SelectStatement;
import com.db.parser.createStatement;
import com.db.storageManager.Disk;
import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;
import com.db.storageManager.SchemaManager;

public class TestTwoPassNaturalJoin {

	static SchemaManager schema_manager;
	
	public static void main(String[] args) {
		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		schema_manager = new SchemaManager(mem, disk);
		
		
		
		// process the create statement
		createStatement create = new createStatement();
		
		Relation relation_reference = create.parseCreateStatement(mem, "create TABLE students (id INT,name STR20,age INT, marks INT,height INT,surname STR20,college STR20 ,address STR20)", schema_manager);
		//Relation relation_reference = create.parseCreateStatement(mem, "create TABLE tablename(id INT,name str20, s INT,  c INT, d INT, h INT, y INT, t INT)", schema_manager);
		//String statementType = getStatementType("CREATE TABLE tablename(name INT, id str20)");
		
		//***********************************************************************************************
		// process the insert statement
//		InsertStatement insert = new InsertStatement();
//		for(int i = 0; i < 4; i++) {
//			insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(1, \"Sukhdeep\")");
//			insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(2, \"Christopher\")");
//			insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(2, \"Chris\")");
//			System.out.println(i);
//		}
//		
//		SelectStatement select = new SelectStatement();
//		select.parseSelectStatement(mem,schema_manager, "SELECT * from tablename" );
	}

}
