package com.gm;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by jason on 17-7-25.
 */
public class PyUtil {
    private static final Log LOG = LogFactory.getLog("com.gm.PyUtil");
    //用于存放function的名字
    private static List<String> funcList = new ArrayList<>();

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
        LOG.info("------kpiNO " + kpiNo);
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
        hqlCore = hqlCore.replaceAll(";", "");
        String funcName = tableName + "_" + kpiNo;
        funcList.add(funcName.trim());
        //System.out.println(funcName + " $1" + " $2");
        LOG.info("functionName: " + funcName);
        writer.println("#" + line0 + "\n" +
                        "def " + funcName + "(begin_day, end_day):\n" +
                        "\tline = \"\"\"" + hqlCore + "\"\"\"\n" +
                        "\tmon_p = begin_day + '_' + end_day\n" +
                        "\tline = line.replace('${begin_date}', begin_day)\n" +
                        "\tline = line.replace('${end_date}', end_day)\n" +
                        "\tline = line.replace('${mon_p}', mon_p)\n" +
                        "\tline = line.replace('${pub_db}',pub_db)\n" +
                        "\ttry:\n" +
                        "\t\tspark.sql(line)\n" +
                        "\texcept:\n" +
                        "\t\ttraceback.print_exc()\n"

        );

    }

    //用于生成基盘类脚本中的function
    public static void makeJPL(List<String> list, PrintWriter writer) {
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
        LOG.info("------kpiNO " + kpiNo);
        //从第二行获得表名
        String line2 = list.get(1);
        indexSrc = line2.indexOf("table") + 5;
        indexDest = line2.length();
        tableName = line2.substring(indexSrc, indexDest).trim();

        //添加partition
        String partition = " partition(region='${region}',mon_p='${mon_p}',kpi='" + kpiNo + "')";
        list.remove(0);
        list.set(0, "  " + list.get(0) + partition);
        String hqlCore = StringUtils.join(list, "\n  ");
        hqlCore = hqlCore.replaceAll(";", "");
        String funcName = tableName + "_" + kpiNo;
        funcList.add(funcName.trim());
        //System.out.println(funcName + " $1" + " $2");
        LOG.info("functionName: " + funcName);
        writer.println("#" + line0 + "\n" +
                        "def " + funcName + "(region,begin_day, end_day):\n" +
                        "\tline = \"\"\"" + hqlCore + "\"\"\"\n" +
                        "\tmon_p = begin_day + '_' + end_day\n" +
                        "\tline = line.replace('${begin_date}', begin_day)\n" +
                        "\tline = line.replace('${end_date}', end_day)\n" +
                        "\tline = line.replace('${mon_p}', mon_p)\n" +
                        "\tline = line.replace('${pub_db}',pub_db)\n" +
                        "\tline = line.replace('${region}',region)\n" +
                        "\tline = line.replace('${district}', district)\n" +
                        "\tline = line.replace('${everymonth}',everymonth)\n" +
                        "\ttry:\n" +
                        "\t\tspark.sql(line)\n" +
                        "\texcept:\n" +
                        "\t\ttraceback.print_exc()\n"

        );

    }

    //用于生成脚本中的function（非基盘类）
    public static void makeFJP(List<String> list, PrintWriter writer) {
        String userName = "ipsos";
        String kpiNo;
        String tableName;
        //获取代码注释当做ｆｕｎｃｔｉｏｎ名字
        //从第一行获得kpi编号
        String line0 = list.get(0);
        int indexSrc = line0.lastIndexOf("----") + 4;
        int indexDest = line0.indexOf("(");
        kpiNo = line0.substring(indexSrc, indexDest).trim();
        if (kpiNo.contains(".")) {
            kpiNo = kpiNo.replaceAll("\\.", "_");
        }
        LOG.info("------kpiNO " + kpiNo);
        //从第二行获得表名
        String line2 = list.get(1);
        indexSrc = line2.indexOf("table") + 5;
        indexDest = line2.length();
        tableName = line2.substring(indexSrc, indexDest).trim();

        //删除第一行注释
        list.remove(0);
        String hqlCore = StringUtils.join(list, "\n  ");
        hqlCore = hqlCore.replaceAll(";", "");
        String funcName = tableName + "_" + kpiNo;
        ;
        funcList.add(funcName.trim());
        //System.out.println(funcName + " $1" + " $2");
        LOG.info("functionName: " + funcName);
        writer.println("#" + line0 + "\n" +
                        "def " + funcName + "(region, pub_db):\n" +
                        "\tline = \"\"\"" + hqlCore + "\"\"\"\n" +
                /*"\tmon_p = begin_day + '_' + end_day\n" +*/
                        "\tline = line.replace('${region}', region)\n" +
                        "\tline = line.replace('${pub_db}',pub_db)\n" +
                        "\tline = line.replace('${district}', district)\n" +
                        "\tline = line.replace('${everymonth}',everymonth)\n" +
                        "\ttry:\n" +
                        "\t\tspark.sql(line)\n" +
                        "\texcept:\n" +
                        "\t\ttraceback.print_exc()\n"

        );

    }


    /*生成脚本开头*/
    public static void printStart(PrintWriter writer) {
        writer.println("#coding=utf-8");
        writer.println("import sys");
        writer.println("import traceback");
        writer.println("import datetime");
        writer.println("reload(sys)");
        writer.println("sys.setdefaultencoding('utf8')");
        writer.println("from pyspark import SparkConf, SparkContext");
        writer.println("from pyspark.sql import *");
        writer.println("sconf = SparkConf().setAppName(\"ipsos-kpi\")");
        writer.println("sc = SparkContext(conf=sconf)");
        writer.println("sc.setLogLevel(\"WARN\")");
        writer.println("spark = HiveContext(sc)");
        writer.println("basepath = '/home/ipsos_test1/general'");
        writer.println("pub_db = 'ori'");
        writer.println("district = '8areas'");
        writer.println("everymonth = 'everymonth'");

        /*writer.println("spark.sql(\"add jar /data2/u_lx_tst2/test/hive_udf_ideal_20140307.jar\");");
        writer.println("spark.sql(\"create temporary function getBase64 as 'com.ideal.hive.udf.BASE64DE'\")");*/


        //writer.println("spark.sql(\"add jar /home/ipsos/general/bin/transform-date.jar\")");
        writer.println("spark.sql(\"add jar \" + basepath + \"/bin/transform-date.jar\")");
        writer.println("spark.sql(\"CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff'\")");
        writer.println("spark.sql(\"CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate'\")");
        writer.println("spark.sql(\"set hive.exec.dynamic.partition.mode=nonstrict\")");
        writer.println("spark.sql(\"set spark.sql.crossJoin.enabled=true\")");

        //writer.println("spark.sql(\"use u_lx_tst2\")");
        writer.println("spark.sql(\"use ipsos_test1\")");
        writer.println();


    }

    /*生成 function run_all*/
    public static void printAllFunc(PrintWriter writer) {
        funcList.set(0, " " + funcList.get(0));
        writer.println("function run_all() {\n" +
                StringUtils.join(funcList, " $1 $2\n ") +
                " $1 $2\n " +
                "\n}");
    }

    //供printend comparator 使用
    public static String parseFunc(String s) {
        int kpi_index = s.lastIndexOf("kpi");
        int bracket_index = s.length();

        return s.substring(kpi_index, bracket_index);
    }

    public static void printEnd(PrintWriter writer) {
        //二次加工funclist,准备为每个kpi的计算结束后添加结束标记
        List<String> list = new ArrayList<>();
        String funRef = "(begin_day,end_day)";
        Collections.sort(funcList, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return parseFunc(o1).compareTo(parseFunc(o2));
            }
        });
        for (String s : funcList) {

            String starttime = "startime = datetime.datetime.now()";
            String func = s + funRef;
            String endTime = "endtime = datetime.datetime.now()";
            //String overMark = "print \"--------" + s + "\"";
            String overMark = "time_record.write(\"" + s + "\\t\" + startime.strftime(\"%Y-%m-%d %H:%M:%S\") + \"\\t\" + endtime.strftime(\"%Y-%m-%d %H:%M:%S\") + \"\\t\" + str((endtime - startime).seconds) + \"s\\n\" )";
            list.add(starttime);
            list.add(func);
            list.add(endTime);
            list.add(overMark);
        }
        writer.println("if __name__ == \"__main__\":\n" +
                        "\tif sys.argv.__len__() != 3:\n" +
                        "\t\tprint(\"需要两个参数 <开始时间> <结束时间>\")\n" +
                        "\t\tsys.exit(-1)\n" +
                        "\tbegin_day = sys.argv[1]\n" +
                        "\tend_day = sys.argv[2]\n" +
                        "\ttime_record = open(basepath + \"/log/time_record\", \"w\")\n" +
                        "\t" + StringUtils.join(list, "\n\t") +
                        "\n\ttime_record.close()"
        );
    }

    //非基盘类end 的打印
    public static void printEndFJP(PrintWriter writer) {
        //二次加工funclist,准备为每个kpi的计算结束后添加结束标记
        List<String> list = new ArrayList<>();
        String funRef = "(region,pub_db)";
        for (String s : funcList) {

            String starttime = "startime = datetime.datetime.now()";
            String func = s + funRef;
            String endTime = "endtime = datetime.datetime.now()";
            //String overMark = "print \"--------" + s + "\"";
            String overMark = "time_record.write(\"" + s + "\\t\" + startime.strftime(\"%Y-%m-%d %H:%M:%S\") + \"\\t\" + endtime.strftime(\"%Y-%m-%d %H:%M:%S\") + \"\\t\" + str((endtime - startime).seconds) + \"s\\n\" )";
            list.add(starttime);
            list.add(func);
            list.add(endTime);
            list.add(overMark);
        }
        writer.println("if __name__ == \"__main__\":\n" +
                        "\tif sys.argv.__len__() != 3:\n" +
                        "\t\tprint(\"需要两个参数 <region> <pub_db>\")\n" +
                        "\t\tsys.exit(-1)\n" +
                        "\tregion = sys.argv[1]\n" +
                        "\tpub_db = sys.argv[2]\n" +
                        "\ttime_record = open(basepath + \"/log/time_record\", \"w\")\n" +
                        "\t" + StringUtils.join(list, "\n\t") +
                        "\n\ttime_record.close()"
        );
    }


    //基盘类end 的打印
    public static void printEndJPL(PrintWriter writer) {
        //二次加工funclist,准备为每个kpi的计算结束后添加结束标记
        List<String> list = new ArrayList<>();
        String funRef = "(region,begin_day,end_day)";
        Collections.sort(funcList, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return parseFunc(o1).compareTo(parseFunc(o2));
            }
        });
        for (String s : funcList) {

            String starttime = "startime = datetime.datetime.now()";
            String func = s + funRef;
            String endTime = "endtime = datetime.datetime.now()";
            //String overMark = "print \"--------" + s + "\"";
            String overMark = "time_record.write(\"" + s + "\\t\" + startime.strftime(\"%Y-%m-%d %H:%M:%S\") + \"\\t\" + endtime.strftime(\"%Y-%m-%d %H:%M:%S\") + \"\\t\" + str((endtime - startime).seconds) + \"s\\n\" )";
            list.add(starttime);
            list.add(func);
            list.add(endTime);
            list.add(overMark);
        }
        writer.println("if __name__ == \"__main__\":\n" +
                        "\tif sys.argv.__len__() != 4:\n" +
                        "\t\tprint(\"需要三个参数 <region> <begin_day> <end_day>\")\n" +
                        "\t\tsys.exit(-1)\n" +
                        "\tregion = sys.argv[1]\n" +
                        "\tbegin_day = sys.argv[2]\n" +
                        "\tend_day = sys.argv[3]\n" +
                        "\ttime_record = open(basepath + \"/log/time_record\", \"w\")\n" +
                        "\t" + StringUtils.join(list, "\n\t") +
                        "\n\ttime_record.close()"
        );
    }

    public static int getFuncSize() {
        return funcList.size();
    }
}
