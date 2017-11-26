package com.db.parser;

import com.db.storageManager.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class parser {
	static SchemaManager schema_manager;

	public static void main(String[] args) {
		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		schema_manager = new SchemaManager(mem, disk);

		// String[] tokens= lex("create TABLE course ( sid INT , homework INT , project
		// INT , exam INT , grade STR20 )");
		//String[] tokens = lex("create TABLE t1 ( id INT , name STR20 )");
		
		//Relation tableName = parse(tokens);
		
		
		//***********************************************************************************************
		// process the create statement
		createStatement create = new createStatement();
		Relation relation_reference = create.parseCreateStatement(mem, "create TABLE tablename(id INT, name str20)", schema_manager);
		//String statementType = getStatementType("CREATE TABLE tablename(name INT, id str20)");
		
		//***********************************************************************************************
		// process the insert statement
		InsertStatement insert = new InsertStatement();
		insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(1, \"Sukhdeep\")");
		insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(2, \"Christopher\")");

		SelectStatement select = new SelectStatement();
		select.parseSelectStatement(mem,schema_manager, "SELECT id, name      from tablename" );
		
		//***********************************************************************************************
		// process the drop statement
		//dropStatement drop = new dropStatement();
		//drop.parseDeleteStatement("drop table tablename", schema_manager);
		
		//***********************************************************************************************
		// process the delete statement
		deleteStatement delete = new deleteStatement();
		delete.parseDeleteStatement("DELETE FROM tablename", schema_manager, mem);
		insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(1, \"Sukhdeep\")");
		insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(2, \"Christopher\")");
		select.parseSelectStatement(mem,schema_manager, "SELECT id, name      from tablename" );


		ArrayList<String> inputList = new ArrayList<>();
		/*inputList.add("1");
		inputList.add("+");
		inputList.add("2");
		inputList.add("*");
		inputList.add("4");
		inputList.add("/");
		inputList.add("5");
		inputList.add("-");
		inputList.add("7");
		inputList.add("+");
		inputList.add("3");
		inputList.add("/");
		inputList.add("6");*/

		inputList.add("name");
		inputList.add("=");
		inputList.add("\"chris\"");
		inputList.add("OR");
		inputList.add("age");
		inputList.add("*");
		inputList.add("weight");
		inputList.add("=");
		inputList.add("100");
		inputList.add("AND");
		inputList.add("marks");
		inputList.add("*");
		inputList.add("3");
		inputList.add(">");
		inputList.add("150");

		ArrayList<String> output;
		whereClause wc = new whereClause();
		output = wc.inToPost(inputList);
		System.out.println("Postfix is " + output + '\n');

		//String str = "(\"hello\"  +  \"there\")";
		//System.out.println("Input is " + str);
		//wc.tokenizeWhere(str);
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
		switch (statementType) {
		case "create":
			createStatement create = new createStatement();
			create.parseCreateStatement(mem, "create TABLE tablename(name INT, id str20)", schema_manager);
		case "insert":
			InsertStatement insert = new InsertStatement();
			//insert.parseInsertStatement(relation_reference, mem, "insert into tablename(id, name) VALUES(1, \"Sukhdeep\")");		
		case "select":
			
		case "drop":
			dropStatement drop = new dropStatement();
			drop.parseDeleteStatement(statement, schema_manager);
		case "delete":	
			deleteStatement delete = new deleteStatement();
			delete.parseDeleteStatement("DELETE FROM tablename", schema_manager, mem);
		}
	}
}
