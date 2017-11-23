package com.db.parser;

import java.util.Arrays;

public class Helper {

    public static String[] trimAndSplitByComma(String str) {

        // remove all the spaces before and after the given string
        String trimmed = str.trim();

        // get all the tokens by spliting by commma
        String[] arr = trimmed.split(",");

        // remove all the spaces before and after each token
        for (int i = 0; i < arr.length; i++) {
            arr[i] = arr[i].trim();

        }
        //System.out.println("trimAndSplitByComma: "+Arrays.toString(arr));
        return arr;
    }
}
