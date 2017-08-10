package com.gm;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jason on 2017-04-07.
 */
public class CreateShell {
    //用于存放function的名字
    private static List<String> funcList = new ArrayList<>();

    //用于生成脚本
    public static void createShell(String filename) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("kpi.sh"), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        printStart(writer);
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("----")) {
                flag = true;
                list = new ArrayList<>();
            }
            if (flag) {
                list.add(line);
            }
            if (line.endsWith(";")) {
                flag = false;
                //处理
                make(list, writer);
                list = null;
            }
        }
        printAllFunc(writer);
        printEnd(writer);
        reader.close();
        writer.close();
    }

    //用于生成脚本中的function
    public static void make(List<String> list, PrintWriter writer) {
        String userName = "ipsos";
        String kpiNo;
        String tableName;
        //从第一行获得kpi编号
        String line0 = list.get(0);
        int indexSrc = line0.lastIndexOf("----") + 4;
        int indexDest = line0.indexOf("(");
        kpiNo = line0.substring(indexSrc, indexDest).trim();
        if (kpiNo.contains(".")) {
            kpiNo = kpiNo.replaceAll("\\.", "_");
        }

        //从第二行获得表名
        String line2 = list.get(1);
        indexSrc = line2.indexOf("table") + 5;
        indexDest = line2.length();
        tableName = line2.substring(indexSrc, indexDest).trim();

        //添加partition
        String partition = " partition(mon_p='${mon_p}',kpi='${kpi}')";
        list.remove(0);
        list.set(0, "  " + list.get(0) + partition);
        String hqlCore = StringUtils.join(list, "\n  ");
        String funcName = tableName + "_" + kpiNo;
        funcList.add(funcName);
        System.out.println(funcName + " $1" + " $2");
        writer.println("#" + line0 + "\n" +
                "function " + funcName + "(){\n" +
                " begin_date=$1\n" +
                " end_date=$2\n" +
                " if [ ${begin_date} = ${end_date} ] \n" +
                " then\n" +
                "  mon_p=${end_date}\n" +
                " else\n" +
                "  mon_p=${begin_date}_${end_date}\n" +
                " fi\n" +
                " kpi=\"" + kpiNo + "\"\n" +
                //" hive -e \"\n" +
                "  spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e \"\n" +
                "  use " + userName + ";\n" +
                /*"  set mapreduce.map.cpu.vcores=2;\n" +
                "  set mapreduce.map.memory.mb=4096;\n" +
                "  set mapreduce.map.java.opts=-Xmx3072m;\n" +
                "  set mapreduce.reduce.cpu.vcores=4;\n" +
                "  set mapreduce.reduce.memory.mb=8192;\n" +
                "  set mapreduce.reduce.java.opts=-Xmx6144m;\n" +
                "  set hive.exec.compress.intermediate=true;\n" +
                "  set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;\n" +
                "  set hive.exec.compress.output=true;\n" +
                "  set mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;\n" +
                "  set hive.exec.mode.local.auto=true;\n" +*/
                "  set spark.sql.crossJoin.enabled=true;\n" +
                "  add jar /home/" + userName + "/general/bin/transform-date.jar;\n" +
                "  CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';\n" +
                "  CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';\n" +
                "  alter table " + tableName + " drop if exists partition(mon_p='${mon_p}',kpi='${kpi}');\n" +
                "  alter table " + tableName + " add partition (mon_p='${mon_p}',kpi='${kpi}') location '${mon_p}/${kpi}';\n" +
                hqlCore +
                " \" > ../log/" + tableName + "_${mon_p}_${kpi} 2>&1\n" +
                "  echo \"" + tableName + "_${mon_p}_${kpi}  $?\" >> ../log/res\n" +
                "}\n");
    }

    /*生成脚本开头*/
    public static void printStart(PrintWriter writer) {
        writer.println("#!/bin/bash");
        writer.println("source /etc/profile\n");
    }

    /*生成 function run_all*/
    public static void printAllFunc(PrintWriter writer) {
        funcList.set(0, " " + funcList.get(0));
        writer.println("function run_all() {\n" +
                StringUtils.join(funcList, " $1 $2\n ") +
                " $1 $2\n " +
                "\n}");
    }

    public static void printEnd(PrintWriter writer) {
        funcList.add("run_all");
        writer.println("\nif [ $# -lt 3 ] \n" +
                "then\n" +
                " echo \"Usage: sh $0 [" + StringUtils.join(funcList, " | ") + "] <start_date> <end_date>\"\n" +
                " exit 1\n" +
                "fi\n" +
                "#rm ../log/res\n" +
                "$1 $2 $3");
    }

    public static void main(String[] args) throws IOException {
        createShell("hql");
        System.out.println(funcList.size());
    }
}
