package com.alg;

import java.util.StringTokenizer;

/**
 * Created by konglu on 2016/8/3.
 */
public class TestStringTokenizer {
    public static void main(String[] args){
//        StringBuilder sb=new StringBuilder("hello   world");
//        StringTokenizer st=new StringTokenizer(sb.toString());
//        while(st.hasMoreTokens()){
//            System.out.println(st.nextToken());
//        }
        int res=testFinally();
        System.out.println(res);
    }
    public static int testFinally(){
        int result=1;
        try{
            result=2;
            return result;
        }catch (Exception e){
            return 0;
        }finally {
            result=3;
            int i=1;
            i++;
            System.out.println("execute finally"+"i: "+i);
        }
    }
}
