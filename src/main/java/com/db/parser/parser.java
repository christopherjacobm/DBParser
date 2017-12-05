package com.db.parser;

import com.db.storageManager.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class parser {
	static SchemaManager schema_manager;
	static FileWriter fw;
	static BufferedWriter bw;

	public static void main(String[] args) {
		MainMemory mem = new MainMemory();
		Disk disk = new Disk();
		schema_manager = new SchemaManager(mem, disk);
		
		//***********************************************************************************************

		double time = disk.getDiskTimer();
		long ios = disk.getDiskIOs();

		try {  //make a new output file
			File outFile = new File("Output.txt");
			boolean res = Files.deleteIfExists(outFile.toPath());
			//System.out.println("file deletion successful? :"+res);
		} catch (IOException e) {
			e.printStackTrace();
		}

		Scanner sc = new Scanner(System.in);
		boolean quit=false;

		while (!quit){
			System.out.println("Enter query / f to read from file / q to quit. ");
			String input = sc.nextLine();


			try{ fw = new FileWriter("Output.txt",true); } //Create fileWriter to append output of select statements to outputfile
			catch (IOException e) { e.printStackTrace(); }
			if (fw!=null) bw = new BufferedWriter(fw);

			switch(input){
				case "f":
					System.out.println("Input file name");
					String fileName = sc.nextLine();
					File file = new File(fileName);
					ArrayList<String> statements = Helper.readStatementsFromFile(file);
					for (String statement: statements){
						executeStatement(getStatementType(statement),statement,mem,disk);
					}
					break;
				case "q":
					quit=true;
					break;
				default://tinysql statement
					executeStatement(getStatementType(input),input,mem,disk);
			}

			try {
				bw.flush();
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

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
			//System.out.println("value is " + match.group(1));
			return  match.group(1);
		}
		return null;
		
	}
	
	public static void executeStatement(String statementType, String statement, MainMemory mem, Disk disk) {
		//System.out.println("executestatement for: "+statementType+" stmnt: "+statement);
		if (statementType!=null) {
			switch (statementType.toLowerCase()) {
				case "create":
					createStatement create = new createStatement();
					create.parseCreateStatement(mem, statement, schema_manager);
					break;
				case "insert":
					InsertStatement insert = new InsertStatement();
					insert.parseInsertStatement(mem, statement, schema_manager);
					break;
				case "select":
					double time = disk.getDiskTimer();
					long ios = disk.getDiskIOs();
					SelectStatement select = new SelectStatement(bw);
					select.parseSelectStatement(mem, schema_manager, statement);
					System.out.printf("Select Time: %.2f ms", (disk.getDiskTimer() - time));
					System.out.println("\nSelect IO's: " + (disk.getDiskIOs() - ios));
					System.out.println();
					break;
				case "drop":
					dropStatement drop = new dropStatement();
					drop.parseDeleteStatement(statement, schema_manager);
					break;
				case "delete":
					deleteStatement delete = new deleteStatement();
					delete.parseDeleteStatement(statement, schema_manager, mem);
					break;
			}
		}
	}
}
