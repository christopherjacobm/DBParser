package com.db.parser;

import com.db.operations.*;
import com.db.storageManager.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectStatement {
    BufferedWriter bw;
    public SelectStatement( BufferedWriter bw) {
        // default constructor
        this.bw = bw;
    }

    public void parseSelectStatement(MainMemory mem, SchemaManager schemaManager, String statement) {
        String attributes = "(\\s*[a-z][a-z.0-9]*\\s*(?:,\\s*[a-z][a-z.0-9]*\\s*)*)";
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
            //System.out.println("tables: "+tables);

            /* System.out.println("groupcount: "+match.groupCount());
            System.out.println("group 3: "+match.group(3));
            System.out.println("group 4: "+match.group(4));
            System.out.println("group 5: "+match.group(5));
            System.out.println("group 6: "+match.group(6));
            System.out.println("group 7: "+match.group(7));
            System.out.println("group 8: "+match.group(8)); */

            if (match.group(5)!=null){//where exists
                where=true;
                wc = new whereClause(match.group(6));
            }

            if (match.group(7)!=null){//order by exists
                orderBy=true;
                orderByColumnName = match.group(8).trim();
                //System.out.println("orderByColumnName: "+orderByColumnName);
            }

            //=================PARSING DONE========================
            //todo in case of cross join/ nat join, have to keep ALL COMMON COLUMN NAMES in t.name style attributes
            //first remove all tablenames from attribute names for new table
            //todo add tablename to newtable's schema for common attributes
            if (tables.size()>1){//MULTI TABLE CASE
                //todo print column titles
                //System.out.println("multi table");

                ArrayList<Tuple> result = CrossJoin.crossJoin(schemaManager.getRelation(tables.get(0)),schemaManager.getRelation(tables.get(1)),mem,schemaManager);
                System.out.println(result.size());
                //filter array by where first?? projection last??

                if (where) result = applyWhere(result,wc);

                Relation tempRel = null;

                if(!result.isEmpty()) {

                    if (distinct || orderBy) {
                        tempRel = writeToNewTempRelation(mem, schemaManager, result, result.get(0).getSchema(), tables.get(0) + tables.get(1));
                    }

                    if (distinct && !orderBy) {
                        System.out.println("Calling distinct now");
                        if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                        else result = Distinct.distinct(tempRel, mem, attrs);
                        projectAndOutput(result, attrs, star);
                    }

                    if (orderBy && !distinct) {
                        ArrayList<String> sortColumn = new ArrayList<>();
                        sortColumn.add(orderByColumnName);
                        result = Sort.sort(tempRel, mem, sortColumn);
                        projectAndOutput(result, attrs, star);
                    }

                    if (orderBy && distinct) {
                        if (tempRel.getNumOfBlocks() < mem.getMemorySize()) {
                            System.out.println("Calling distinct now");
                            if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                            else result = Distinct.distinct(tempRel, mem, attrs);

                            ArrayList<String> sortColumn = new ArrayList<>();
                            sortColumn.add(orderByColumnName);
                            Collections.sort(result, new CompareTuplesSort(sortColumn));

                            //result = Sort.sort(tempRel, mem, sortColumn);
                        } else { //2 pass
                            System.out.println("Calling  2 pass distinct now");
                            if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                            else result = Distinct.distinct(tempRel, mem, attrs);

                            Relation tempRelDistinct = writeToNewTempRelation(mem, schemaManager, result, tempRel.getSchema(), tempRel.getRelationName() + "distinct");

                            ArrayList<String> sortColumn = new ArrayList<>();
                            sortColumn.add(orderByColumnName);
                            result = Sort.sort(tempRelDistinct, mem, sortColumn);
                        }

                        projectAndOutput(result, attrs, star);
                    }

                }


                //todo - uncomment after testing cross join
               /* if (where){
                    String naturalJoinColumn = wc.getNaturalJoinAttribute();
                    if(naturalJoinColumn!=null){
                        //natural join can be performed
                        NaturalJoin.naturalJoin(mem, schemaManager, tables.get(0), tables.get(1), naturalJoinColumn );
                    }
                } */



                //get arraylist result=natural join and newly generated relation name. use these to do distinct, order by. probably same for cross join

            } else {//SINGLE TABLE CASE

                String relName = tables.get(0);
                Relation r = schemaManager.getRelation(relName);
                Schema schema = r.getSchema();
                int numBlocksInRelation = r.getNumOfBlocks();

                //from attrs, orderby attr, remove dot and everything before it
                //for(int i=0;i<attrs.size();i++){
                //    attrs.set(i,Helper.removeTableNameAndDotIfExists(attrs.get(i)));
                //}
                //if (orderBy) orderByColumnName = Helper.removeTableNameAndDotIfExists(orderByColumnName);

                //System.out.println("attrs after removal: "+attrs);
                //System.out.println("orderByColumnName after removal: "+orderByColumnName);

                //=================PRINTING HEADINGS====================

                printToConsoleAndFile("\n========================================\n");

                //printing the column titles
                ArrayList<String> field_names = schema.getFieldNames();
                if (star) {//for select *
                    for (String fName : field_names) {
                        printToConsoleAndFile(fName + " ");
                    }
                    printToConsoleAndFile("\n");
                } else {
                    for (String attr : attrs) {
                        printToConsoleAndFile(attr + " ");
                    }
                    printToConsoleAndFile("\n");
                }

                printToConsoleAndFile("========================================\n");

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
                Relation tempRel = null;
                Block lastBlock = null;
                //if (distinct or order) make temp relation
                if (distinct || orderBy) {
                    if (!star) {
                        for (String fName : relevantFieldNames)
                            relevantFieldTypes.add(schema.getFieldType(fName));
                        //make a schema with only relevant fields
                        Schema schemaTemp = new Schema(relevantFieldNames, relevantFieldTypes);
                        //make a temp relation
                        schema = schemaTemp;
                    }
                    String tempRelName = relName + "temp";
                    tempRel = schemaManager.createRelation(tempRelName, schema);
                    System.out.println("tempRel's fields: " + tempRel.getSchema().getFieldNames());
                    lastBlock = mem.getBlock(mem.getMemorySize() - 1); //gets the last block of mem
                    lastBlock.clear();
                }

                //--OPTIMIZING BASED ON PROJECTION DONE


                //System.out.println("tables: " + tables);
                //System.out.println("attrs: " + attrs);
                //System.out.println("relevantFieldNames: " + relevantFieldNames);


                //todo distinct, order by
                //todo case sensitive everywhere in parsing!
                int blocksLeft = numBlocksInRelation;
                //int relationBlockIndex;
                int memSize;
                if (distinct || orderBy)
                    memSize = mem.getMemorySize() - 1; //9 blocks for reading, 1 for the temporary relation
                else memSize = mem.getMemorySize();

                int numBlocksToRead;

                //System.out.println("blocksLeft: " + blocksLeft);

                while (blocksLeft > 0) {
                    //System.out.println("in loop, blocksLeft: "+blocksLeft);
                    if (blocksLeft > memSize) numBlocksToRead = memSize;
                    else numBlocksToRead = blocksLeft;
                    r.getBlocks(numBlocksInRelation - blocksLeft, 0, numBlocksToRead);
                    blocksLeft -= numBlocksToRead;

                    ArrayList<Tuple> tuples = mem.getTuples(0, numBlocksToRead);

                    //System.out.println("before loop, where: " + where);

                    Field f;
                    for (Tuple t : tuples) {
                        if ((!where) || (where && wc.satisfiedByTuple(t))) {//if the where condition is true for this tuple, print it

                            if (star) {//for select *
                                int numFields = t.getNumOfFields();
                                //System.out.println("in star numfields: "+numFields);

                                if (distinct || orderBy) {
                                    //add this field after checking type


                                    if (!lastBlock.isFull()) lastBlock.appendTuple(t);
                                    else {
                                        mem.setBlock(mem.getMemorySize() - 1, lastBlock);
                                        tempRel.setBlock(tempRelBlockIndex++, mem.getMemorySize() - 1);
                                        lastBlock.clear();
                                        lastBlock.appendTuple(t);
                                    }
                                } else {
                                    for (int j = 0; j < numFields; j++) {
                                        f = t.getField(j);
                                        //if (distinct or order) write to temp relation instead of printing
                                        printToConsoleAndFile(f.toString() + " ");
                                    }
                                    printToConsoleAndFile("\n");
                                }
                            } else {//for select a,b

                                if (distinct || orderBy) {
                                    Tuple tempTuple = tempRel.createTuple();
                                    for (String fName : relevantFieldNames) {
                                        f = t.getField(fName);
                                        if (f.type == FieldType.INT)
                                            tempTuple.setField(fName, f.integer);
                                        else
                                            tempTuple.setField(fName, f.str);
                                    }
                                    if (!lastBlock.isFull()) lastBlock.appendTuple(tempTuple);
                                    else {
                                        mem.setBlock(mem.getMemorySize() - 1, lastBlock);
                                        tempRel.setBlock(tempRelBlockIndex++, mem.getMemorySize() - 1);
                                        lastBlock.clear();
                                        lastBlock.appendTuple(tempTuple);
                                    }
                                } else {
                                    for (String fName : relevantFieldNames) {//loop thru relevantfieldnames (same as attrs here)
                                        f = t.getField(fName);
                                        //if (distinct or order) write to temp relation instead of printing
                                        printToConsoleAndFile(f.toString() + " ");
                                    }
                                    printToConsoleAndFile("\n");
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

                    //System.out.println(tempRel.getNumOfTuples());

                    //=========APPLY DISTINCT & ORDER BY==========

                    ArrayList<Tuple> result;

                    if (distinct && !orderBy){
                        System.out.println("Calling distinct now");
                        if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                        else result = Distinct.distinct(tempRel, mem, attrs);

                        // print the distinct tuples;
                        for(Tuple tuple : result) {
                            printToConsoleAndFile(tuple.toString(false));
                            printToConsoleAndFile("\n");
                        }
                    }

                    if (orderBy && !distinct){
                        ArrayList<String> sortColumn = new ArrayList<>();
                        sortColumn.add(orderByColumnName);
                        result = Sort.sort(tempRel, mem, sortColumn);

                        projectAndOutput(result,attrs,star);

                    }

                    if(orderBy && distinct){
                        if(tempRel.getNumOfBlocks() < mem.getMemorySize()) {
                            System.out.println("Calling distinct now");
                            if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                            else result = Distinct.distinct(tempRel, mem, attrs);

                            ArrayList<String> sortColumn = new ArrayList<>();
                            sortColumn.add(orderByColumnName);
                            Collections.sort(result, new CompareTuplesSort(sortColumn));

                            //result = Sort.sort(tempRel, mem, sortColumn);
                        } else { //2 pass
                            System.out.println("Calling  2 pass distinct now");
                            if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                            else result = Distinct.distinct(tempRel, mem, attrs);

                            Relation tempRelDistinct = writeToNewTempRelation(mem,schemaManager,result,tempRel.getSchema(),tempRel.getRelationName()+"distinct");

                            ArrayList<String> sortColumn = new ArrayList<>();
                            sortColumn.add(orderByColumnName);
                            result = Sort.sort(tempRelDistinct, mem, sortColumn);
                        }

                        projectAndOutput(result,attrs,star);
                    }
                }
                printToConsoleAndFile("========================================\n\n");

            }
        }
        else System.out.println("no match");

    }

    private void printToConsoleAndFile(String str){
        System.out.print(str);
        try {if (bw!=null) bw.write(str); }
        catch (IOException e) { e.printStackTrace(); }
    }

    private void projectAndOutput (ArrayList<Tuple> tuples, ArrayList<String> attributes, boolean star){
        for(Tuple tuple : tuples) {
            if(star){
                printToConsoleAndFile(tuple.toString(false));
            } else {
                for (String fName : attributes) {//loop thru attrs
                    String colName = Helper.getColNameMatchingToken(fName,tuple);
                    Field f = tuple.getField(colName);
                    printToConsoleAndFile(f.toString() + " ");
                }
            }
            printToConsoleAndFile("\n");
        }
    }

    private void projectApplyWhereAndOutput (ArrayList<Tuple> tuples, ArrayList<String> attributes, boolean star, boolean where, whereClause wc){
        for(Tuple tuple : tuples) {
            if ((!where) || (where && wc.satisfiedByTuple(tuple))) {
                if (star) {
                    printToConsoleAndFile(tuple.toString(false));
                } else {
                    for (String fName : attributes) {//loop thru attrs
                        String colName = Helper.getColNameMatchingToken(fName, tuple);
                        Field f = tuple.getField(colName);
                        printToConsoleAndFile(f.toString() + " ");
                    }
                }
                printToConsoleAndFile("\n");
            }
        }
    }

    private Relation writeToNewTempRelation(MainMemory mem,SchemaManager schemaManager,ArrayList<Tuple> result,Schema schema,String newRelationName) {
        //write result to new tempRelation?
        if (schemaManager.relationExists(newRelationName))
            schemaManager.deleteRelation(newRelationName);
        Relation tempRel = schemaManager.createRelation(newRelationName,schema);

        int blockCount = 0;
        Block b = mem.getBlock(0);

        while(!result.isEmpty()) {
            b.clear();
            for(int i=0;i<schema.getTuplesPerBlock();i++){
                if(!result.isEmpty()){
                    Tuple t = result.get(0);
                    b.setTuple(i,t);
                    result.remove(t);
                }
            }
            tempRel.setBlock(blockCount++,0);
        }
        return tempRel;
    }

    private ArrayList<Tuple> applyWhere(ArrayList<Tuple> tuples, whereClause wc){
        ArrayList<Tuple> ret = new ArrayList<>();
        for(Tuple tuple : tuples) {
            if (wc.satisfiedByTuple(tuple)) {
                ret.add(tuple);
            }
        }
        return ret;
    }
}
