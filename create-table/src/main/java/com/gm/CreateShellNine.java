package com.gm;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;

/**
 * Created by jason on 2017-06-22.
 */
public class CreateShellNine {
    //用于生成脚本
    public static void createShell(String filename, String outName) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/jason/" + outName), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        ShellUtil.printStart(writer);
        int count = 0;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
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
                ShellUtil.make(list, writer);
                count++;
                list = null;
            }
        }
        ShellUtil.makeFunction(writer);
        ShellUtil.printAllFunc(writer);
        ShellUtil.printEnd(writer);
        reader.close();
        writer.close();
    }

    //非基盘类
    public static void createShellFJP(String filename, String outName) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/jason/" + outName), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        ShellUtil.printStart(writer);
        int count = 0;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
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
                ShellUtil.makeFJP(list, writer);
                count++;
                list = null;
            }
        }
        ShellUtil.makeFunction(writer);
        ShellUtil.printAllFunc(writer);
        ShellUtil.printEnd(writer);
        reader.close();
        writer.close();
    }

    //用于生成基盘类脚本
    public static void createShellJPL(String filename, String outName) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/jason/" + outName), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        ShellUtil.printStart(writer);
        int count = 0;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
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
                ShellUtil.makeJPL(list, writer);
                count++;
                list = null;
            }
        }
        ShellUtil.makeFunction(writer);
        ShellUtil.printAllFunc(writer);
        ShellUtil.printEnd(writer);
        reader.close();
        writer.close();
    }

    //用于生成基盘类脚本,新增8areas和version变量
    public static void createShellnewJPL(String filename, String outName) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\fx\\Desktop\\" + outName), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        ShellUtil.printStart(writer);
        int count = 0;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("----")) {
                flag = true;
                list = new ArrayList<>();
            }
            if (flag) {
                if (line.contains("'8areas'")) {
                    line = line.replaceAll("'8areas'", "\'\\$\\{8areas\\}\'");
                }
                if (line.contains("'everymonth'")) {
                    line = line.replaceAll("'everymonth'", "\'\\$\\{everymonth\\}\'");
                }
                list.add(line);
            }
            if (line.endsWith(";")) {
                flag = false;
                //处理
                ShellUtil.makeJPL(list, writer);
                count++;
                list = null;
            }
        }
        ShellUtil.makeFunction(writer);
        ShellUtil.printAllFunc(writer);
        ShellUtil.printEnd(writer);
        reader.close();
        writer.close();
    }


    public static void main(String[] args) throws IOException {
        //createShell("zhijiexiangjialei", "zhijiexiangjialei.sh");
        //createShell("zhanbilei", "zhanbilei.sh");
//        createShellJPL("jpl", "jpl.sh");
        //createShell("hql3", "kpi_test3.sh");
        createShellnewJPL("fyjp.txt", "fyjpnew.sh");

    }

}
