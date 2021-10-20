package de.tub.dima.babelfish;

import java.io.*;
import java.util.*;

public class JSProgram {

    public static class t {
        t(ArrayList<Integer> integers) {
            int sum = 0;
            for (int i = 0; i < 1000000; i++) {
                sum += sumStream(integers);
            }
            System.out.println(sum);
        }

        public int sumLoop(ArrayList<Integer> list) {
            int sum = 0;
            for (int i = 0; i < list.size(); i++) {
                sum = list.get(i);
            }
            return sum;
        }

        public static int sumStream(ArrayList<Integer> list) {
            int sum = list.stream().reduce(0, (integer, integer2) -> integer + integer2);
            return sum;
        }

        public static int sumIter(ArrayList<Integer> list) {
            int sum = 0;
            for (Integer i : list) {
                sum += i;
            }
            return sum;
        }
    }

    public static int sum(int b){
       int sum = 0;
        for(int i = 0; i< b;i++){
            sum = sum + i;
        }
        return sum;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        ArrayList<Integer> integers = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            integers.add(i);
        }

        System.out.println("run");

        new JSProgram.t(integers);
        new JSProgram.t(integers);
        new JSProgram.t(integers);
        new JSProgram.t(integers);
        new JSProgram.t(integers);


        System.out.println("wait");


        Thread.sleep(100000);

    }

}
