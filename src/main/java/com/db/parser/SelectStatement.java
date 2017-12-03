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
        String regexValue = "^\\s*select(\\s+distinct)?\\s+(\\*|"+attributes+")\\s+from\\s+"+attributes+"\\s*(where)?(.*?)(?=order by|$)(order by)?(.*)?$"; //parse out order by, don't give it to whereclause
        Pattern regex = Pattern.compile(regexValue, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        Matcher match = regex.matcher(statement);

        // if the string is matched
        if (match.find()) {
            //System.out.println("\n Select parse values are " + "\n" + match.group(1) + "\n" + match.group(2) + "\n" + match.group(3) + "\n" + match.group(4));

            boolean distinct=false;
            boolean star=false;
            boolean where=false;
            boolean orderBy=false;
            String orderByColumnName = null;
            whereClause wc=null;
            ArrayList<String> attrs = new ArrayList<>();

            if (match.group(1)!=null)
                distinct = true;

            if(match.group(2).equals("*")){
                star = true;
            } else {
                //group(3) has attributes
                attrs = Helper.trimAndSplitByComma(match.group(3));
            }

            ArrayList<String> tables = Helper.trimAndSplitByComma(match.group(4));
            //System.out.println("tables: "+tables[0]);

            System.out.println("groupcount: "+match.groupCount());
            System.out.println("group 3: "+match.group(3));
            System.out.println("group 4: "+match.group(4));
            System.out.println("group 5: "+match.group(5));
            System.out.println("group 6: "+match.group(6));
            System.out.println("group 7: "+match.group(7));
            System.out.println("group 8: "+match.group(8));

            if (match.group(5)!=null){//where exists
                where=true;
                wc = new whereClause(match.group(6));
            }

            if (match.group(7)!=null){//order by exists
                orderBy=true;
                orderByColumnName = match.group(8).trim();
                System.out.println("orderByColumnName: "+orderByColumnName);
            }


            //System.out.println("output is: " + bool + '\n');

            //=================PRINTING HEADINGS====================

            System.out.println();
            System.out.println("========================================");

                String relName = tables.get(0);
                Relation r = schemaManager.getRelation(relName); //todo-handle cases with more tables
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

            //=======================================================




            //--OPTIMIZING BASED ON PROJECTION - pushing selection?

                //if (order) do union of attrs and order_by_field_name and store in arraylist relevantFieldNames, else relevantFieldNames = attrs
                ArrayList<String> relevantFieldNames = new ArrayList<>(attrs);
                ArrayList<FieldType> relevantFieldTypes = new ArrayList<>();

                if (orderBy) {
                    if (!relevantFieldNames.contains(orderByColumnName)) {
                        relevantFieldNames.add(orderByColumnName); //if not already present, add
                    }
                }

                int tempRelBlockIndex = 0;
                Relation tempRel=null;
                Block lastBlock=null;
                //if (distinct or order) make temp relation
                if (distinct || orderBy){
                    if (!star) {
                        for (String fName : relevantFieldNames)
                            relevantFieldTypes.add(schema.getFieldType(fName));
                        //make a schema with only relevant fields
                        Schema schemaTemp = new Schema(relevantFieldNames, relevantFieldTypes);
                        //make a temp relation
                        schema = schemaTemp;
                    }
                    String tempRelName = relName+"temp";
                    tempRel = schemaManager.createRelation(tempRelName,schema);
                    System.out.println("tempRel's fields: "+tempRel.getSchema().getFieldNames());
                    lastBlock = mem.getBlock(mem.getMemorySize()-1); //gets the last block of mem
                    lastBlock.clear();
                }

            //--OPTIMIZING BASED ON PROJECTION DONE


            System.out.println("tables: "+tables);
            System.out.println("attrs: "+attrs);
            System.out.println("relevantFieldNames: "+relevantFieldNames);


            //todo distinct, order by
                //todo case sensitive everywhere in parsing!
                int blocksLeft = numBlocksInRelation;
                //int relationBlockIndex;
                int memSize;
                if (distinct || orderBy)
                    memSize = mem.getMemorySize()-1; //9 blocks for reading, 1 for the temporary relation
                else memSize = mem.getMemorySize();

                int numBlocksToRead;

                System.out.println("blocksLeft: "+blocksLeft);

                    while (blocksLeft > 0) {
                        //System.out.println("in loop, blocksLeft: "+blocksLeft);
                        if(blocksLeft>memSize)numBlocksToRead = memSize;
                        else numBlocksToRead = blocksLeft;
                        r.getBlocks(numBlocksInRelation-blocksLeft, 0, numBlocksToRead);
                        blocksLeft-=numBlocksToRead;

                        ArrayList<Tuple> tuples = mem.getTuples(0,numBlocksToRead);

                        System.out.println("before loop, where: "+where);

                        Field f;
                        for (Tuple t : tuples) {
                            if ((!where) || (where && wc.satisfiedByTuple(t))) {//if the where condition is true for this tuple, print it
                                if (star) {//for select *
                                    int numFields = t.getNumOfFields();
                                    //System.out.println("in star numfields: "+numFields);

                                    if (distinct || orderBy){
                                        //add this field after checking type
                                        if (!lastBlock.isFull()) lastBlock.appendTuple(t);
                                        else {
                                            mem.setBlock(mem.getMemorySize()-1,lastBlock);
                                            tempRel.setBlock(tempRelBlockIndex++,mem.getMemorySize()-1);
                                            lastBlock.clear();
                                            lastBlock.appendTuple(t);
                                        }
                                    }
                                    else {
                                        for (int j = 0; j < numFields; j++) {
                                            f = t.getField(j);
                                            //if (distinct or order) write to temp relation instead of printing
                                            System.out.print(f.toString() + " ");
                                        }
                                        System.out.println("");
                                    }
                                } else {//for select a,b

                                    if (distinct || orderBy){
                                        Tuple tempTuple = tempRel.createTuple();
                                        for (String fName : relevantFieldNames) {
                                            f = t.getField(fName);
                                            if (f.type==FieldType.INT)
                                                tempTuple.setField(fName,f.integer);
                                            else
                                                tempTuple.setField(fName,f.str);
                                        }
                                        if (!lastBlock.isFull()) lastBlock.appendTuple(tempTuple);
                                        else {
                                            mem.setBlock(mem.getMemorySize()-1,lastBlock);
                                            tempRel.setBlock(tempRelBlockIndex++,mem.getMemorySize()-1);
                                            lastBlock.clear();
                                            lastBlock.appendTuple(tempTuple);
                                        }
                                    }
                                    else {
                                        for (String fName : relevantFieldNames) {//loop thru relevantfieldnames (same as attrs here)
                                            f = t.getField(fName);
                                            //if (distinct or order) write to temp relation instead of printing
                                            System.out.print(f.toString()+" ");
                                        }
                                        System.out.println("");
                                    }
                                }
                            }
                        }

                    }
                    if (distinct || orderBy) {
                        if (!lastBlock.isEmpty()) { //write leftover tuples to temp relation
                            mem.setBlock(mem.getMemorySize() - 1, lastBlock);
                            tempRel.setBlock(tempRelBlockIndex++, mem.getMemorySize() - 1);
                            lastBlock.clear();
                        }

                        //

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
