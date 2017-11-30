package com.db.parser;

import com.db.storageManager.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class parser {
	static SchemaManager schema_manager;

	public static void main(String[] args) {
		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		schema_manager = new SchemaManager(mem, disk);
		
		//Relation tableName = parse(tokens);
		
		//***********************************************************************************************
		// process the create statement
		createStatement create = new createStatement();
		Relation relation_reference = create.parseCreateStatement(mem, "CREATE TABLE students (id INT, name STR20, age INT, marks INT, height INT, surname STR20, college STR20, address STR20)", schema_manager);
		//Relation relation_reference = create.parseCreateStatement(mem, "create TABLE students(id INT, name STR20)", schema_manager);

		double time = disk.getDiskTimer();
		long ios = disk.getDiskIOs();
		//***********************************************************************************************
		// process the insert statement
		InsertStatement insert = new InsertStatement();
		insert.parseInsertStatement(relation_reference, mem, "INSERT INTO students (id,name,age,marks,height,surname,college,address) VALUES(1,\"Sarah\",30,100,163,\"Parker\",\"TAMU\",\"Cherry Street\")");
		insert.parseInsertStatement(relation_reference, mem, "INSERT INTO  students (id,name,age,marks,height,surname,college,address) VALUES(2, \"Chris\", 40, 100, 165,\"Evans\",\"TAMU\",\"Stack\" )");

		System.out.printf("Time: %.2f ms",(disk.getDiskTimer() - time));
		System.out.println("\nIO's: "+(disk.getDiskIOs() - ios));

		//SelectStatement select = new SelectStatement();
		//select.parseSelectStatement(mem,schema_manager, "SELECT id, name      from tablename" );
		
		//***********************************************************************************************
		// process the drop statement
		//dropStatement drop = new dropStatement();
		//drop.parseDeleteStatement("drop table tablename", schema_manager);
		
		//***********************************************************************************************

		String inputQuery = "SELECT id,name FROM students WHERE name = \"Chris\" AND id = 2 AND college = \"TAMU\" AND age*2=80 ";
		executeStatement(getStatementType(inputQuery),inputQuery,mem);

		System.out.printf("Time: %.2f ms",(disk.getDiskTimer() - time));
		System.out.println("\nIO's: "+(disk.getDiskIOs() - ios));

	}
	
	// select the operation using regex
	private static String getStatementType(String statement) {
		String regexValue = "^\\s*(create|insert|select|drop|delete).*";
		Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
		Matcher match = regex.matcher(statement);

		// if the string is matched
		if (match.find()) {
			System.out.println("value is " + match.group(1));
			return  match.group(1);
		}
		return null;
		
	}
	
	public static void executeStatement(String statementType, String statement, MainMemory mem) {
		switch (statementType.toLowerCase()) {
		case "create":
			createStatement create = new createStatement();
			create.parseCreateStatement(mem, statement, schema_manager);
		case "insert":
			InsertStatement insert = new InsertStatement();
			//insert.parseInsertStatement(relation_reference, mem, statement);
		case "select":
			SelectStatement select = new SelectStatement();
			select.parseSelectStatement(mem,schema_manager,statement);
		case "drop":
			dropStatement drop = new dropStatement();
			drop.parseDeleteStatement(statement, schema_manager);
		case "delete":	
			deleteStatement delete = new deleteStatement();
			delete.parseDeleteStatement(statement, schema_manager, mem);
		}
	}
}
