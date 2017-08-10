package com.gm;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;

/**
 * Created by jason on 17-8-1.
 */
public class ShellUtil {
    //用于存放function的名字
    private static List<String> funcList = new ArrayList<>();
    //用于存放所有的hqlcore
    private static Map<String, List<String>> hqlCoreMap = new HashMap<>();


    //用于生成脚本
    public static void createShell(String filename, String outName) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/jason/" + outName), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        printStart(writer);
        int count = 0;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            //test
            System.out.println(line);
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
                count++;
                list = null;
            }
        }
        System.out.println(count);
        makeFunction(writer);
        printAllFunc(writer);
        printEnd(writer);
        reader.close();
        writer.close();
    }

    //将map中按照kpino分好组的数据编写成function
    public static void makeFunction(PrintWriter writer) {
        String excutespark = "  spark-sql --master yarn --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e \"\n";
        String excuteshive = " hive -e \"\n";
        String userName = "ipsos_test1";
        Set<String> keys = hqlCoreMap.keySet();
        List<String> tmplist = new ArrayList<>(keys);
        System.out.println("=====" + tmplist.size());
        Collections.sort(tmplist);
        //Collections.copy(funcList, tmplist);
        funcList.addAll(tmplist);
        for (String s : tmplist) {

            writer.println("function " + s + "(){\n" +
                    " #开始时间\n" +
                    " date1=$(date  +\"%Y-%m-%d %H:%M:%S\")\n" +
                    " time1=$(date +\"%s\")\n" +
                    " begin_date=$1\n" +
                    " end_date=$2\n" +
                    " if [ ${begin_date} = ${end_date} ] \n" +
                    " then\n" +
                    "  mon_p=${end_date}\n" +
                    " else\n" +
                    "  mon_p=${begin_date}_${end_date}\n" +
                    " fi\n" +
                    excutespark +
                    "  use " + userName + ";\n" +
                    "  add jar /home/" + userName + "/general/bin/transform-date.jar;\n" +
                    "  CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';\n" +
                    "  CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';\n" +
                    "  set spark.sql.crossJoin.enabled=true;\n");
            for (String core : hqlCoreMap.get(s)) {
                writer.println(core);
                writer.println();
            }
            writer.println("\" > ../log/" + s + "_${begin_date}_${end_date} 2>&1\n" +
                    "  echo \"" + s + "_${begin_date}_${end_date}  $?\" >> ../log/res_test\n" +
                    "#结束时间\n" +
                    "time2=$(date +\"%s\")\n" +
                    "date2=$(date +\"%Y-%m-%d %H:%M:%S\")\n" +
                    "\n" +
                    "#计算时间\n" +
                    "let time=time2-time1\n" +
                    "let min_time=time/3600\n" +
                    "echo -e \"" + s + "\\t${date1}\\t${date2}\\t${time}s\" >>${recordFile}\n" +
                    "echo \"----------------------------\" >>${recordFile}\n" +
                    " \n" +
                    "}\n");
        }
    }

    //将所有的kpi按照kpino进行分组 放入map
    public static void make(List<String> list, PrintWriter writer) {
        String userName = "ipsos";
        String kpiNo;
        String tableName;
        //hive
        String hive = " hive -e \"\n";
        String spark = " spark-sql -e \"\n";
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
        String partition = " partition(mon_p='${mon_p}',kpi='" + kpiNo + "')";
        list.remove(0);
        list.set(0, "  " + list.get(0) + partition);
        String hqlCore = StringUtils.join(list, "\n  ");
        //System.out.println(hqlCore);
        String funcName = tableName + "_" + kpiNo;
        //funcList.add(funcName);
        //System.out.println(funcName + " $1" + " $2");
        if (hqlCoreMap.containsKey(kpiNo)) {
            hqlCoreMap.get(kpiNo).add(hqlCore);
        } else {
            List<String> hqlCoreList = new ArrayList<>();
            hqlCoreList.add(hqlCore);
            hqlCoreMap.put(kpiNo, hqlCoreList);
        }
    }

    //将所有的kpi按照kpino进行分组 放入map  基盘类
    public static void makeJPL(List<String> list, PrintWriter writer) {
        String userName = "ipsos";
        String kpiNo;
        String tableName;
        //hive
        String hive = " hive -e \"\n";
        String spark = " spark-sql -e \"\n";
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
        String partition = " partition(region='${region},'mon_p='${mon_p}',kpi='" + kpiNo + "')";
        list.remove(0);
        list.set(0, "  " + list.get(0) + partition);
        String hqlCore = StringUtils.join(list, "\n  ");
        //System.out.println(hqlCore);
        String funcName = tableName + "_" + kpiNo;
        //funcList.add(funcName);
        //System.out.println(funcName + " $1" + " $2");
        if (hqlCoreMap.containsKey(kpiNo)) {
            hqlCoreMap.get(kpiNo).add(hqlCore);
        } else {
            List<String> hqlCoreList = new ArrayList<>();
            hqlCoreList.add(hqlCore);
            hqlCoreMap.put(kpiNo, hqlCoreList);
        }
    }

    //将所有的kpi按照kpino进行分组 放入map 非基盘类
    public static void makeFJP(List<String> list, PrintWriter writer) {
        String userName = "ipsos";
        String kpiNo;
        String tableName;
        //hive
        String hive = " hive -e \"\n";
        String spark = " spark-sql -e \"\n";
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
        String partition = " partition(mon_p='${mon_p}',kpi='" + kpiNo + "')";
        list.remove(0);
        list.set(0, "  " + list.get(0));
        String hqlCore = StringUtils.join(list, "\n  ");
        //System.out.println(hqlCore);
        String funcName = tableName + "_" + kpiNo;
        //funcList.add(funcName);
        //System.out.println(funcName + " $1" + " $2");
        if (hqlCoreMap.containsKey(kpiNo)) {
            hqlCoreMap.get(kpiNo).add(hqlCore);
        } else {
            List<String> hqlCoreList = new ArrayList<>();
            hqlCoreList.add(hqlCore);
            hqlCoreMap.put(kpiNo, hqlCoreList);
        }
    }


    /*生成脚本开头*/
    public static void printStart(PrintWriter writer) {
        writer.println("#!/bin/bash");
        writer.println("source /etc/profile\n");
        writer.println("recordFile=../log/time_record");
        writer.println("pub_db=ori");
        writer.println("region=");
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
}
