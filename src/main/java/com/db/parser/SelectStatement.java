package com.db.parser;

import com.db.storageManager.*;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectStatement {

    public SelectStatement() {
        // default constructor
    }

    // insert statement
    public void parseSelectStatement(MainMemory mem, SchemaManager schemaManager, String statement) {
        String attributes = "(\\s*[a-z][a-z0-9]*\\s*(?:,\\s*[a-z][a-z0-9]*\\s*)*)";
        String regexValue = "^\\s*select(\\s+distinct)?\\s+(\\*|"+attributes+")\\s+from\\s+"+attributes+"\\s*$";
        Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher match = regex.matcher(statement);

        // if the string is matched
        if (match.find()) {
            System.out.println("\n Select parse values are " + "\n" + match.group(1) + "\n" + match.group(2) + "\n" + match.group(3) + "\n" + match.group(4));

            boolean distinct=false;
            boolean star=false;
            String[] attrs = new String[10];

            if (match.group(1)!=null)
                distinct = true;

            if(match.group(2).equals("*")){
                star = true;
            } else {
                //group(3) has attributes
                attrs = Helper.trimAndSplitByComma(match.group(3));
            }

            String[] tables = Helper.trimAndSplitByComma(match.group(4));
            //System.out.println("tables: "+tables[0]);

            //=======================================================

            System.out.println();
            System.out.println("========================================");

            for (int i=0;i<tables.length;i++){

                Relation r = schemaManager.getRelation(tables[i]);
                Schema schema = r.getSchema();
                int numBlocks = r.getNumOfBlocks();
                //int numTuples = r.getNumOfTuples();

                r.getBlocks(0,0,numBlocks); //TODO handle cases where relation>10 blocks
                Block b = mem.getBlock(0);
                ArrayList<Tuple> tuples= b.getTuples();
                ArrayList<String> field_names=schema.getFieldNames();



                if (star) {//for select *
                    for (String fName : field_names) {
                        System.out.print(fName + " ");
                    }
                    System.out.println();
                } else {
                    for (String attr : attrs) {
                        System.out.print(attr + " ");
                    }
                    System.out.println();
                }

                System.out.println("----------------------------------------");

                Field f;
                for(Tuple t  : tuples){

                    if (star) {//for select *
                        int numFields = t.getNumOfFields();
                        for (int j = 0; j < numFields; j++) {
                            f = t.getField(j);
                            System.out.print(f.toString() + " ");
                        }
                        System.out.println("");
                    } else {//for select a,b
                        for (String attr : attrs) {
                            int j = schema.getFieldOffset(attr);
                            f = t.getField(j);
                            System.out.print(f.toString() + " ");
                        }
                        System.out.println("");
                    }
                }
            }

            System.out.println("========================================");
            System.out.println();

        }
        else System.out.println("no match");
    }


}
