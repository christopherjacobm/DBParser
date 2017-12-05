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

            if (match.group(5)!=null){//where exists
                where=true;
                wc = new whereClause(match.group(6));
            }

            if (match.group(7)!=null){//order by exists
                orderBy=true;
                orderByColumnName = match.group(8).trim();
            }

            //=================PARSING DONE========================
            //todo add tablename to newtable's schema for common attributes

            if (tables.size()>1){//MULTI TABLE CASE
                //todo print column titles
                //System.out.println("multi table");

                ArrayList<Tuple> result = null;


                if (where && wc.getNaturalJoinAttribute()!=null) {
                        //natural join can be performed
                    result = NaturalJoin.naturalJoin(mem, schemaManager, tables.get(0), tables.get(1),wc.getNaturalJoinAttribute() );
                } else {//do cross join
                    result = CrossJoin.crossJoin(schemaManager.getRelation(tables.get(0)),schemaManager.getRelation(tables.get(1)),mem,schemaManager);
                }

                //System.out.println(result.size());
                //filter array by where first?? projection last??

                if (where) result = applyWhere(result,wc);

                Relation tempRel = null;

               if(!result.isEmpty()) {

                    if (distinct || orderBy) {
                        tempRel = writeToNewTempRelation(mem, schemaManager, result, result.get(0).getSchema(), tables.get(0) + tables.get(1));
                    }

                    if (distinct && !orderBy) {
                        if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                        else result = Distinct.distinct(tempRel, mem, attrs);
                    }

                    if (orderBy && !distinct) {
                        ArrayList<String> sortColumn = new ArrayList<>();
                        sortColumn.add(orderByColumnName);
                        result = Sort.sort(tempRel, mem, sortColumn);
                    }

                    if (orderBy && distinct) {
                        if (tempRel.getNumOfBlocks() < mem.getMemorySize()) {
                            if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                            else result = Distinct.distinct(tempRel, mem, attrs);

                            ArrayList<String> sortColumn = new ArrayList<>();
                            sortColumn.add(orderByColumnName);
                            Collections.sort(result, new CompareTuplesSort(sortColumn));

                            //result = Sort.sort(tempRel, mem, sortColumn);
                        } else { //2 pass
                            if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                            else result = Distinct.distinct(tempRel, mem, attrs);

                            Relation tempRelDistinct = writeToNewTempRelation(mem, schemaManager, result, tempRel.getSchema(), tempRel.getRelationName() + "distinct");

                            ArrayList<String> sortColumn = new ArrayList<>();
                            sortColumn.add(orderByColumnName);
                            result = Sort.sort(tempRelDistinct, mem, sortColumn);
                        }

                    }

                }
                printToConsoleAndFile("\n========================================\n");
                outputColumnTitles(result, attrs, star);
                printToConsoleAndFile("----------------------------------------\n");
                projectAndOutput(result, attrs, star);
                printToConsoleAndFile("========================================\n\n");
                //get arraylist result=natural join and newly generated relation name. use these to do distinct, order by. probably same for cross join

            } else {//SINGLE TABLE CASE
                //System.out.println("Single table case!");

                String relName = tables.get(0);
                Relation r = schemaManager.getRelation(relName);
                Schema schema = r.getSchema();
                int numBlocksInRelation = r.getNumOfBlocks();

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

                printToConsoleAndFile("-------------------------------------------\n");

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
                            relevantFieldTypes.add(schema.getFieldType(Helper.removeTableNameAndDotIfExists(fName)));
                        //make a schema with only relevant fields
                        Schema schemaTemp = new Schema(relevantFieldNames, relevantFieldTypes);
                        //make a temp relation
                        schema = schemaTemp;
                    }
                    String tempRelName = relName + "temp";
                    tempRel = schemaManager.createRelation(tempRelName, schema);
                    lastBlock = mem.getBlock(mem.getMemorySize() - 1); //gets the last block of mem
                    lastBlock.clear();
                }

                //--OPTIMIZING BASED ON PROJECTION DONE


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
                                        f = t.getField(Helper.removeTableNameAndDotIfExists(fName));
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
                                        f = t.getField(Helper.removeTableNameAndDotIfExists(fName));
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
                            if (star) result = Distinct.distinct(tempRel, mem, tempRel.getSchema().getFieldNames());
                            else result = Distinct.distinct(tempRel, mem, attrs);

                            ArrayList<String> sortColumn = new ArrayList<>();
                            sortColumn.add(orderByColumnName);
                            Collections.sort(result, new CompareTuplesSort(sortColumn));

                            //result = Sort.sort(tempRel, mem, sortColumn);
                        } else { //2 pass
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

    private void outputColumnTitles (ArrayList<Tuple> tuples, ArrayList<String> attributes, boolean star){
            Tuple tuple = tuples.get(0);
            if(star){
                for (String fName: tuple.getSchema().getFieldNames()) printToConsoleAndFile(fName);
            } else {
                for (String fName : attributes) {//loop thru attrs
                    String colName = Helper.getColNameMatchingToken(fName,tuple);
                    printToConsoleAndFile(colName+ " ");
                }
            }
            printToConsoleAndFile("\n");
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
