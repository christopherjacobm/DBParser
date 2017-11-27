package com.db.parser;

import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;

public class whereClause {


    public ArrayList<String> tokenizeWhere(String input){
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

    private Stack<String> theStack;
    //private String output = "";
    private ArrayList<String> outputList = new ArrayList();

    public ArrayList<String> inToPost(ArrayList<String> tokens) {
        theStack = new Stack<>();
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
                    theStack.push(token);
                    break;
                case ")":
                    gotParen(token);
                    break;
                default:
                    outputList.add(token);
                    break;

            }
        }
        while (!theStack.isEmpty()) {
            outputList.add(theStack.pop());
        }
        return outputList;
    }


    public void gotOper(String opThis, int prec1) {
        while (!theStack.isEmpty()) {
            String opTop = theStack.pop();
            if (opTop == "(") {
                theStack.push(opTop);
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
                    theStack.push(opTop);
                    break;
                }
                else outputList.add(opTop);
            }
        }
        theStack.push(opThis);
    }
    public void gotParen(String str) {
        while (!theStack.isEmpty()) {
            String chx = theStack.pop();
            if (chx == "(")
                break;
            else outputList.add(chx);
        }
    }
}
