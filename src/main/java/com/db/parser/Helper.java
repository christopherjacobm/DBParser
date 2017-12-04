package com.db.parser;

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
}
