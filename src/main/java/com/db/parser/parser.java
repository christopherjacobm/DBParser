package com.db.parser;

import com.db.storageManager.*;
import java.util.ArrayList;
import java.util.Arrays;

public class parser {
    static SchemaManager schema_manager;
    public static void main(String[] args ){
        MainMemory mem=new MainMemory();
        Disk disk=new Disk();
        schema_manager=new SchemaManager(mem,disk);

        //String[] tokens= lex("create TABLE course ( sid INT , homework INT , project INT , exam INT , grade STR20 )");
        String[] tokens = lex("create TABLE t1 ( c INT )");
        parse(tokens);
    }

    public static String[] lex(String str) {
        String arr[] = str.split(" "); //can't always split by spaces in case of comma-separated attributes
       // for (String temp : arr) {
         //   System.out.println(temp);
        //}
        return arr;
    }

    public static void parse(String[] arr) {
        switch (arr[0]) {
            case "create":
                String[] createArr = Arrays.copyOfRange(arr, 2, arr.length);
                create(createArr);
            break;
        }
    }

    public static void create(String[] arr) {
        System.out.println("create called with array: ");
        for (String str: arr)
            System.out.println(str);
        String relation_name = arr[0];
        ArrayList<String> field_names=new ArrayList<String>();
        ArrayList<FieldType> field_types=new ArrayList<FieldType>();
        if (arr[1].equals("(")){
            String[] attributeTypeListArr = Arrays.copyOfRange(arr, 2, arr.length-1);
            attributeTypeList(attributeTypeListArr,field_names,field_types);
        }
        createTable(relation_name,field_names,field_types);
    }

    public static void attributeTypeList(String[] arr,ArrayList<String> field_names,ArrayList<FieldType> field_types){
        if (arr.length>2){//there are more attributes
            parseFirstFieldNameAndFieldType(arr,field_names,field_types);
            //remove first 3 elements of array and recurse
            String[] attributeTypeListArr = Arrays.copyOfRange(arr, 3, arr.length);
            attributeTypeList(attributeTypeListArr,field_names,field_types);
        } else {//last attribute
            parseFirstFieldNameAndFieldType(arr,field_names,field_types);
        }
    }

    private static void parseFirstFieldNameAndFieldType(String[] arr,ArrayList<String> field_names,ArrayList<FieldType> field_types){
        field_names.add(arr[0]);
        if (arr[1].equals("STR20"))
            field_types.add(FieldType.STR20);
        else if (arr[1].equals("INT"))
            field_types.add(FieldType.INT);
    }

    public static void createTable(String relation_name,ArrayList<String> field_names,ArrayList<FieldType> field_types){
        System.out.print("In createTable");
        Schema schema=new Schema(field_names,field_types);

        Relation relation_reference=schema_manager.createRelation(relation_name,schema);

        // Print the information about the Relation
        System.out.print("The table has name " + relation_reference.getRelationName() + "\n");
        System.out.print("The table has schema:" + "\n");
        System.out.print(relation_reference.getSchema() + "\n");
        System.out.print("The table currently have " + relation_reference.getNumOfBlocks() + " blocks" + "\n");
        System.out.print("The table currently have " + relation_reference.getNumOfTuples() + " tuples" + "\n" + "\n");
    }
}
