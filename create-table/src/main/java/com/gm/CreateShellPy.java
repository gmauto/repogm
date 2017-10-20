package com.gm;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by jason on 2017-06-15.
 */
public class CreateShellPy {
    private static String path = "C:\\Users\\fx\\Desktop\\";

    //用于生成脚本（夏家银）类型
    public static void createShell(String filename, String outname) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/jason/" + outname), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        PyUtil.printStart(writer);
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
                PyUtil.make(list, writer);
                list = null;
            }
        }
        //printAllFunc(writer);
        PyUtil.printEnd(writer);
        reader.close();
        writer.close();
    }

    //用于生成基盘类
    public static void createShellJPL(String filename, String outname) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/jason/" + outname), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        PyUtil.printStart(writer);
        while ((line = reader.readLine()) != null) {
            line = line.trim();
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
                PyUtil.makeJPL(list, writer);
                list = null;
            }
        }
        //printAllFunc(writer);
        PyUtil.printEndJPL(writer);
        reader.close();
        writer.close();
    }

    //非基盘类
    public static void createShellFJP(String filename, String outname) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path + outname), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        PyUtil.printStart(writer);
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
                PyUtil.makeFJP(list, writer);
                list = null;
            }
        }
        //printAllFunc(writer);
        PyUtil.printEndFJP(writer);
        reader.close();
        writer.close();
    }

    //非基盘类，新增8areas变量和时间维度分区
    public static void createShellnewFJP(String filename, String outname) throws IOException {
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path + outname), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        PyUtil.printStart(writer);
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("----")) {
                flag = true;
                list = new ArrayList<>();
            }
            if (flag) {
                if (line.contains("'8areas'")) {
                    line = line.replaceAll("'8areas'", "\'\\$\\{district\\}\'");
                }
                if (line.contains("'everymonth'")) {
                    line = line.replaceAll("'everymonth'", "\'\\$\\{everymonth\\}\'");
                }
                list.add(line);
            }
            if (line.endsWith(";")) {
                flag = false;
                //处理
                PyUtil.makeFJP(list, writer);
                list = null;
            }
        }
        //printAllFunc(writer);
        PyUtil.printEndFJP(writer);
        reader.close();
        writer.close();
    }


    //用于生成基盘类，新增version变量和8areas变量
    public static void createShellnewJPL(String filename, String outname) throws IOException {
//        System.out.println("hcdshcbdhcbhdsbc");
        InputStream in = CreateShell.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        String path = "C:\\Users\\Administrator\\Desktop\\";
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path + outname), "utf-8")), true);
        String line;
        boolean flag = false;
        List<String> list = null;
        PyUtil.printStart(writer);
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            System.out.println(line);
            if (line.startsWith("----")) {
                flag = true;
                list = new ArrayList<>();
            }
            if (flag) {
                if (line.contains("'8areas'")) {
                    line = line.replaceAll("'8areas'", "\'\\$\\{district\\}\'");
                }
                if (line.contains("'everymonth'")) {
                    line = line.replaceAll("'everymonth'", "\'\\$\\{everymonth\\}\'");
                }
                list.add(line);
            }
            if (line.endsWith(";")) {
                flag = false;
                //处理
                PyUtil.makeJPL(list, writer);
                list = null;
            }
        }
        //printAllFunc(writer);
        PyUtil.printEndJPL(writer);
        reader.close();
        writer.close();
    }

    public static void main(String[] args) throws IOException {
//        createShell("kpi2new.txt", "kpi2new.py");
        /*createShell("zjxj","zjxj.py");*/
//        createShellnewFJP("jieguo.txt","jieguo.py");
        createShellnewJPL("fyjp.txt", "fyjp.py");
        //createShellFJP("fjp", "fjp.py");
//        createShellJPL("jpl","jpl.py");
        System.out.println("number of function" + PyUtil.getFuncSize());

    }
}
