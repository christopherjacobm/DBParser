package com.db.parser;

import com.db.storageManager.*;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.db.tree.Node;

public class SelectStatement {

    public SelectStatement() {
        // default constructor
    }

    public void parseSelectStatement(MainMemory mem, SchemaManager schemaManager, String statement) {
        String attributes = "(\\s*[a-z][a-z0-9]*\\s*(?:,\\s*[a-z][a-z0-9]*\\s*)*)";
        String regexValue = "^\\s*select(\\s+distinct)?\\s+(\\*|"+attributes+")\\s+from\\s+"+attributes+"\\s*(where)?(.*)?$";
        Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher match = regex.matcher(statement);

        // if the string is matched
        if (match.find()) {
            //System.out.println("\n Select parse values are " + "\n" + match.group(1) + "\n" + match.group(2) + "\n" + match.group(3) + "\n" + match.group(4));

            boolean distinct=false;
            boolean star=false;
            boolean where=false;
            whereClause wc=null;
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

            //System.out.println("groupcount: "+match.groupCount());
            //System.out.println("group 5: "+match.group(5));
            //System.out.println("group 6: "+match.group(6));

            if (match.group(5)!=null){//where exists
                where=true;
                wc = new whereClause(match.group(6));
            }

            //System.out.println("output is: " + bool + '\n');

            //=======================================================

            System.out.println();
            System.out.println("========================================");

                Relation r = schemaManager.getRelation(tables[0]); //todo-handle cases with more tables
                Schema schema = r.getSchema();
                int numBlocksInRelation = r.getNumOfBlocks();

                //printing the column titles
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

                //todo distinct, order by
                //todo case sensitive everywhere in parsing!
                int blocksLeft = numBlocksInRelation;
                //int relationBlockIndex;
                int memSize = mem.getMemorySize();
                int numBlocksToRead;

                //System.out.println("blocksLeft: "+blocksLeft);

                    while (blocksLeft > 0) {
                        //System.out.println("in loop, blocksLeft: "+blocksLeft);
                        if(blocksLeft>memSize)numBlocksToRead = memSize;
                        else numBlocksToRead = blocksLeft;
                        r.getBlocks(numBlocksInRelation-blocksLeft, 0, numBlocksToRead);
                        blocksLeft-=numBlocksToRead;

                        ArrayList<Tuple> tuples = mem.getTuples(0,numBlocksToRead);

                        Field f;
                        for (Tuple t : tuples) {
                            if ((!where) || (where && wc.satisfiedByTuple(t))) {//if the where condition is true for this tuple, print it
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

                    }

                    System.out.println("========================================");
                    System.out.println();
        }
        else System.out.println("no match");
    }


    /*public Node makeTree(String groupOne, String groupTwo, String[] attrs, String groupFour) {
        Node select = new Node("SELECT");
        Node distinct = null;
        Node[] attributes = null;
        if (groupOne !=null) {
            distinct = new Node("DISTINCT");
            select.getChildren().add(distinct);
        }
        Node attribute_names = new Node("ATTRIBUTE_NAMES");
        select.getChildren().add(attribute_names);

        for(int i = 0; i <attrs.length; i++) {
            attributes[i] = new Node(attrs[i]);
        }

    		//for(int i = 0; i <attributes.length; i++ ) {
                //select.getChildren(0).
            //}

        if (groupTwo !=null) {
            Node selectStar = new Node("PROJECTION");
            select.getChildren().add(selectStar);
        }
        select.getChildren().add(attribute_names);

        return null;
    }
    */
}
