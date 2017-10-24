package com.gm;

import java.io.PrintWriter;

/**
 * Created by jason on 2017-06-26.
 */
public class Utils {
    /*生成脚本开头*/
    public static void printStart(PrintWriter writer) {
        writer.println("#!/bin/bash");
        writer.println("source /etc/profile\n");
        writer.println("recordFile=../log/time_record");
    }


}
