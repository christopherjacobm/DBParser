package com.db.parser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WhereStatement {

	public static void main(String[] args) {
	
		// String to be scanned to find the pattern.
		
		String s1 = "SELECT course.sid, course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid";
		
		String s2 = "SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid";
		
		String s3 = "SELECT * FROM course, course2 WHERE course.sid = course2.sid ORDER BY course.exam";
		
		String s4 = "SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam = 100 AND course2.exam = 100";
		
		String s5 = "SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam = 100 OR course2.exam = 100 ]";
		
		String s6 = "SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam";
		
		String s7 = "SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam AND course.homework = 100";
		
		String s8 = "SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam > course2.exam OR course.homework = 100 ]";
		
		String s9 = "SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam > course2.exam OR course.grade = \"A\" AND course2.grade = ( A + B + C ) ] ORDER BY course.exam";
		
		String[] array = { s1, s2, s3, s4, s5, s6, s7, s8, s9 };
		
		
		
		String pattern = "\\s*((?:([a-z]\\w*)\\.)?\\(?\\s*(\"?\\w+\"?(?:\\s*(?:\\+|\\*|\\-)\\s*\"?\\w+\"?\\s*)*)\\)?\\s*(=|<|>)\\s*(?:([a-z]\\w*)\\.)?\\(?\\s*(\"?\\w+\"?(?:\\s*(?:\\+|\\*|\\-)\\s*\"?\\w+\"?\\s*)*)\\)?)(?:\\s+(and|or)\\s+)?";
		
		// Create a Pattern object
		
		Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		
		Matcher m = null;
		
		for (String s : array) {
		
		m = r.matcher(s);
		
		//System.out.println(s + "\n\t WHERE CLAUSE is " + m.group(0) + "\n");
		
		// important part 
		
		while (m.find()) {
		
		// table name LHS
		
		String table_name_lhs = m.group(2);
		
		
		// column name LHS
		
		String column_name_lhs = m.group(3);
		
		
		// > or < or =
		
		String operator = m.group(4);
		
		
		// table name RHS
		
		String table_name_rhs = m.group(5);
		
		
		// column name RHS
		
		String column_name_rhs = m.group(6);
		
		
		String boolean_operator = m.group(7);
		
		
		System.out.println(s + "\n\t table_lhs: " + table_name_lhs + " | " +  
		
		"column_lhs: "  + column_name_lhs + " | " +  
		
		"operator: "  + operator + " | " +  
		
		"table_rhs: "  + table_name_rhs +  " | " +  
		
		"column_rhs: "  + column_name_rhs +  " | " +  
		
		"boolean operator: "  + boolean_operator + "\n");
		
		}	
		m.reset(); // you dont need this, for this example only	
		}
	}



}