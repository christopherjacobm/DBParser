package com.db.parser;

import com.db.storageManager.MainMemory;
import com.db.storageManager.Relation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectStatement {

    public SelectStatement() {
        // default constructor
    }

    // insert statement
    public void parseSelectStatement(MainMemory mem, String statement) {
        String attributesInBrackets = "\\((\\s*[a-z][a-z0-9]\\s(?:,\\s*[a-z][a-z0-9]\\s)*)\\)";
        String regexValue = "^\\s*select\\s+(distinct)?\\s+(\\|"+attributesInBrackets+")\\s+from\\s+"+attributesInBrackets+"\\s$";
        Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher match = regex.matcher(statement);

        // if the string is matched
        if (match.find()) {
            System.out.println("value is " + match.group(1) + " " + match.group(2) + " " + match.group(3) + " " + match.group(4));
            //String[] attributeNames = trimAndSplitByComma(match.group(2));
            //String[] attributeValues = trimAndSplitByComma(match.group(3));

            // create a tuple with given values
            //createTuple(ref, mem, attributeNames, attributeValues);
        }
        else System.out.println("no match");
    }


}
