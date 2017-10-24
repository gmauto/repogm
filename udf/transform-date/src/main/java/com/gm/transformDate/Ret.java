package com.gm.transformDate;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 如果传入一个参数是 null 则返回0
 * Created by jason on 2017-04-25.
 */
public class Ret extends UDF {
    public double evaluate(String s) {
        if (s == null) {
            return 0d;
        }
        double i;
        try {
            i = Double.valueOf(s);
        } catch (Exception e) {
            return 0d;
        }
        return i;
    }

    public static void main(String[] args) {
        System.out.println(new Ret().evaluate("2.0"));
    }
}
