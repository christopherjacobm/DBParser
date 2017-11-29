package com.db.parser;

import com.db.storageManager.FieldType;
import com.db.storageManager.Tuple;

import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;

public class whereClause {
    //when created, takes a query as input and stores the tokens in its where clause in postfix form. has a method
    private ArrayList<String> tokensInPostFix;

    private Stack<String> inToPostStack;
    private ArrayList<String> inToPostList = new ArrayList();

    public enum TokenType{
        INTEGER, LITERAL, COL_NAME
    }

    public whereClause(String query) {
    //todo see if only where part of query should be passed
        ArrayList<String> tokens = tokenize(query);
        tokensInPostFix = inToPost(tokens);
    }

    private ArrayList<String> tokenize(String input){
        ArrayList<String> tokens = new ArrayList<>();
        String pattern = "\\s*(?:\\(?\\s*(?:(\\\"?[\\w\\.]+\\\"?)(?:\\s*(\\+|\\*|\\-)\\s*(\\\"?[\\w\\.]+\\\"?)\\s*)*)\\)?\\s*)(=|<|>)\\s*(?:\\(?\\s*(?:(\\\"?[\\w\\.]+\\\"?)(?:\\s*(\\+|\\*|\\-)\\s*(\\\"?[\\w\\.]+\\\"?)\\s*)*)\\)?)(?:\\s+(and|or)\\s+)?";

        Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(input);

        while (m.find()) {
            for (int i=1;i<=+m.groupCount();i++) {
                //System.out.println(i+" - "+m.group(i));
                String token = m.group(i);
                if (token != null)
                    tokens.add(token);
            }
        }
        return tokens ;
    }

//============================================================

    private ArrayList<String> inToPost(ArrayList<String> tokens) {
        inToPostStack = new Stack<>();
        for (int j = 0; j < tokens.size(); j++) {
            String token = tokens.get(j);
            switch (token) {
                case "AND":
                case "OR":
                    gotOper(token, 1);
                    break;
                case "=":
                case ">":
                case "<":
                    gotOper(token, 2);
                    break;
                case "+":
                case "-":
                    gotOper(token, 3);
                    break;
                case "*":
                    gotOper(token, 4);
                    break;
                case "(":
                    inToPostStack.push(token);
                    break;
                case ")":
                    gotParen(token);
                    break;
                default:
                    inToPostList.add(token);
                    break;

            }
        }
        while (!inToPostStack.isEmpty()) {
            inToPostList.add(inToPostStack.pop());
        }
        return inToPostList;
    }


    private void gotOper(String opThis, int prec1) {
        while (!inToPostStack.isEmpty()) {
            String opTop = inToPostStack.pop();
            if (opTop == "(") {
                inToPostStack.push(opTop);
                break;
            } else {
                int prec2;

                switch (opTop) {
                    case "OR":
                        prec2 = 0;
                        break;
                    case "AND":
                        prec2 = 1;
                        break;
                    case "=":
                    case ">":
                    case "<":
                        prec2 = 2;
                        break;
                    case "+":
                    case "-":
                        prec2 = 3;
                        break;
                    case "*":
                        prec2 = 4;
                        break;
                    default:
                        prec2= 0;
                }
                if (prec2 < prec1) {
                    inToPostStack.push(opTop);
                    break;
                }
                else inToPostList.add(opTop);
            }
        }
        inToPostStack.push(opThis);
    }
    private void gotParen(String str) {
        while (!inToPostStack.isEmpty()) {
            String chx = inToPostStack.pop();
            if (chx == "(")
                break;
            else inToPostList.add(chx);
        }
    }

//============================================================

    public boolean satisfiedByTuple(Tuple t) {//todo tablename.attrname cases
        //evaluate the postfix where cluase for the given tuple
        if (tokensInPostFix == null) {
            System.out.println("ERROR: no postfix expression found!");
            return false;
        }

        Stack<String> stack = new Stack();

        int right,left,i;
        boolean boolRight,boolLeft,bool;
        for(String token:tokensInPostFix){
            switch(token){
                case "OR":
                    boolRight = Boolean.parseBoolean(stack.pop());
                    boolLeft = Boolean.parseBoolean(stack.pop());
                    bool = boolLeft || boolRight;
                    stack.push(Boolean.toString(bool));
                    break;
                case "AND":
                    boolRight = Boolean.parseBoolean(stack.pop());
                    boolLeft = Boolean.parseBoolean(stack.pop());
                    bool = boolLeft && boolRight;
                    stack.push(Boolean.toString(bool));
                    break;
                case "=":
                    bool = getEqsValue(stack.pop(),stack.pop(),t);
                    stack.push(Boolean.toString(bool));
                    break;
                case ">":
                    right = getIntValue(stack.pop(),t);
                    left = getIntValue(stack.pop(),t);
                    bool = left>right;
                    stack.push(Boolean.toString(bool));
                    break;
                case "<":
                    right = getIntValue(stack.pop(),t);
                    left = getIntValue(stack.pop(),t);
                    bool = left<right;
                    stack.push(Boolean.toString(bool));
                    break;
                case "+":
                    right = getIntValue(stack.pop(),t);
                    left = getIntValue(stack.pop(),t);
                    i = left+right;
                    stack.push(Integer.toString(i));
                    break;
                case "-":
                    right = getIntValue(stack.pop(),t);
                    left = getIntValue(stack.pop(),t);
                    i = left-right;
                    stack.push(Integer.toString(i));
                    break;
                case "*":
                    right = getIntValue(stack.pop(),t);
                    left = getIntValue(stack.pop(),t);
                    i = left*right;
                    stack.push(Integer.toString(i));
                    break;
                default:
                    //means it is an attr_name/int/literal
                    stack.push(token);
            }
        }
        return Boolean.parseBoolean(stack.pop());
    }

    private TokenType getTokenType(String token){
        if (token.contains("\""))
            return TokenType.LITERAL;
        else if (token.matches("-?\\d+"))
            return TokenType.INTEGER;
        else
            return TokenType.COL_NAME;
    }

    private int getIntValue(String token, Tuple t){
        if (getTokenType(token)==TokenType.INTEGER)
            return Integer.parseInt(token);
        else if (getTokenType(token)==TokenType.COL_NAME) {
            FieldType fType = t.getSchema().getFieldType(token);
            if (fType == FieldType.INT) {
                return t.getField(token).integer;
            }
        }
        System.out.println("ERROR: Trying integer operation on string!");
        return -1;
    }

    private String getStringValue(String token, Tuple t){
        if (getTokenType(token)==TokenType.LITERAL) {
            //remove quotes
            token = token.substring(1,token.length()-1);
            //System.out.println("token is a literal: "+token);
            return token;
        } else if (getTokenType(token)==TokenType.COL_NAME) {
            FieldType fType = t.getSchema().getFieldType(token);
            if (fType == FieldType.STR20) {
                //System.out.println("token is a column string: "+t.getField(token).str);
                return t.getField(token).str;
            }
        }
        System.out.println("ERROR: Trying string operation on integer!");
        return null;
    }

    private boolean getEqsValue(String tokenRight, String tokenLeft, Tuple t){
        TokenType rType = getTokenType(tokenRight);
        TokenType lType = getTokenType(tokenLeft);

        if(rType==TokenType.INTEGER || lType==TokenType.INTEGER){
            return getIntValue(tokenLeft,t) == getIntValue(tokenRight,t);
        } else if (rType==TokenType.LITERAL || lType==TokenType.LITERAL){
            return getStringValue(tokenLeft,t).equals(getStringValue(tokenRight,t));
        } else if (rType==TokenType.COL_NAME || lType==TokenType.COL_NAME){ //atleast one is a column name
            //find one which is a column name
            String colToken;
            if (rType==TokenType.COL_NAME)
                colToken=tokenRight;
            else
                colToken = tokenLeft;
            //check if it is INT or STR20
            FieldType fType = t.getSchema().getFieldType(colToken);
            if (fType == FieldType.INT) {
                return getIntValue(tokenLeft,t) == getIntValue(tokenRight,t);
            } else {
                return getStringValue(tokenLeft,t).equals(getStringValue(tokenRight,t));
            }
        } else {
            System.out.println("ERROR: Comparing String with Integer!");
            return false;
        }

    }
}
