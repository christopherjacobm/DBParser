package com.db.parser;

import com.db.storageManager.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class Helper {

    public static ArrayList<String> trimAndSplitByComma(String str) {

        // remove all the spaces before and after the given string
        String trimmed = str.trim();

        // get all the tokens by spliting by commma
        String[] arr = trimmed.split(",");

        // remove all the spaces before and after each token
        for (int i = 0; i < arr.length; i++) {
            arr[i] = arr[i].trim();

        }
        //System.out.println("trimAndSplitByComma: "+Arrays.toString(arr));
        return new ArrayList(Arrays.asList(arr));
    }

    public static ArrayList<String> readStatementsFromFile(File f) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(f));
            ArrayList<String> statements = new ArrayList<>();
            String statement;
            while ((statement=br.readLine())!=null){
                statements.add(statement);
            }
            br.close();
            return statements;
        }
        catch(IOException e){
            e.printStackTrace();
            return null;
        }
        finally {
            if (br!=null){
                try {
                    br.close();
                } catch(IOException e){
                    e.printStackTrace();
                }
            }
        }

    }

    public static String removeTableNameAndDotIfExists(String str){ //if the string is of the form table1.name, it returns name.
        int dotIndex = str.indexOf('.');
        if (dotIndex == -1) return str;
        else return str.substring(dotIndex+1);
    }

    public static String getColNameMatchingToken(String token,Tuple t){
        if (t.getSchema().fieldNameExists(token)) //cases like (token=name, schema contains name) or (token=table1.name, schema contains table1.name)
            return token;
        else if (t.getSchema().fieldNameExists(Helper.removeTableNameAndDotIfExists(token))) // cases like (token=table1.name and schema contains name) (single table)
            return Helper.removeTableNameAndDotIfExists(token);
        else return null;//error case
    }
}
