package com.gm.label.util;

import com.gm.label.model.DossMarkClass;
import com.gm.label.model.OrderMarkClass;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jason on 2017-03-31.
 */
public class Utils {
    //加载机滤 机油配置文件
    public static Map<String, Set<String>> loadMap(String filename) throws IOException {
        //InputStream in = Utils.class.getClassLoader().getResourceAsStream(filename);
        InputStream in = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        String s;
        String[] arr;
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            arr = s.split("\\t", -1);
            if (arr.length != 2) {
                continue;
            }
            String partNum = arr[0].trim().toLowerCase();
            String version = arr[1];
            if (map.containsKey(version)) {
                map.get(version).add(partNum);
            } else {
                Set<String> set = new HashSet<String>();
                set.add(partNum);
                map.put(version, set);
            }

        }
        reader.close();
        return map;
    }

    //日期转换 将表中日期转换为我们要求的日期
    //01JUN2010:00:00:00
    public static String dateTransform(String date) {
        String regex = "\\d{4}-\\d{2}-\\d{2}.*";
        if (date.matches(regex)) {
            return date.substring(0, 10);
        } else {
            String ymd = date.split(":", 2)[0].toLowerCase();
            if (ymd.length() != 9) {
                return null;
            }
            String dd = ymd.substring(0, 2);
            String yy = ymd.substring(5, 9);
            String mm = ymd.substring(2, 5);
            String newMM = null;
            switch (mm) {
                case "jan":
                    newMM = "01";
                    break;
                case "feb":
                    newMM = "02";
                    break;
                case "mar":
                    newMM = "03";
                    break;
                case "apr":
                    newMM = "04";
                    break;
                case "may":
                    newMM = "05";
                    break;
                case "jun":
                    newMM = "06";
                    break;
                case "jul":
                    newMM = "07";
                    break;
                case "aug":
                    newMM = "08";
                    break;
                case "sep":
                    newMM = "09";
                    break;
                case "oct":
                    newMM = "10";
                    break;
                case "nov":
                    newMM = "11";
                    break;
                case "dec":
                    newMM = "12";
                    break;
                default:
                    return null;
            }
            return yy + "-" + newMM + "-" + dd;
        }
    }

    //计算两个日期间的月份差
    public static String getMonthDiff(String s1, String s2) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        Date d1 = null;
        Date d2 = null;
        try {
            d1 = df.parse(s1);
            d2 = df.parse(s2);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        c1.setTime(d1);
        c2.setTime(d2);
        int year1 = c1.get(Calendar.YEAR);
        int year2 = c2.get(Calendar.YEAR);
        int month1 = c1.get(Calendar.MONTH);
        int month2 = c2.get(Calendar.MONTH);
        // 获取年的差值 假设 d1 = 2015-8-16  d2 = 2011-9-30
        int yearInterval = year1 - year2;
        // 获取月数差值
        int monthInterval = month1 - month2;

        return String.valueOf(yearInterval * 12 + monthInterval);
    }

    /**
     * 用来计算车龄类型，会返回一个list，list中存两个String，一个是 0-1，一个是0；
     *
     * @param
     * @return
     */
    public static List<String> getAge(String mon) {
        int i = Integer.valueOf(mon);
        List<String> list = new ArrayList<>();
        String ageType;
        String ageType2;
        if (i < 0) {
            ageType = "0-1年";
            ageType2 = "0年";
        } else {
            int res = i / 12;
            if (res >= 5) {
                ageType = "5+年";
            } else {
                ageType = res + "-" + (res + 1) + "年";
            }
            ageType2 = res + "年";
        }
        list.add(ageType);
        list.add(ageType2);
        return list;
    }

    //加载车型的一级分类和二级分类(order)
    public static Map<String, List<String>> loadMarkClassification(String filename) throws IOException {
        //InputStream in = Utils.class.getClassLoader().getResourceAsStream(filename);
        InputStream in = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        Map<String, List<String>> map = new HashMap<>();
        String s;
        String[] arr;
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            arr = s.split("\\t", -1);
            if (arr.length != 6) {
                continue;
            }
            OrderMarkClass omc = OrderMarkClass.parseOMC(arr);
            if (omc == null) {
                continue;
            }

            String key = omc.getMotorInfo() + omc.getVersion();
            map.put(key, omc.getClassList());
        }
        reader.close();
        return map;
    }

    //加载车型的一级分类和二级分类(doss)
    public static Map<String, List<String>> loadMarkClassificationDoss(String filename) throws IOException {
        //InputStream in = Utils.class.getClassLoader().getResourceAsStream(filename);
        InputStream in = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        Map<String, List<String>> map = new HashMap<>();
        String s;
        String[] arr;
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            arr = s.split("\\t", -1);
            if (arr.length != 5) {
                continue;
            }

            DossMarkClass dmc = DossMarkClass.parseDMC(arr);
            if (dmc == null) {
                continue;
            }
            String key = dmc.getMotorInfo() + dmc.getVersion();
            map.put(key, dmc.getClassList());

        }
        reader.close();
        return map.size() == 0 ? null : map;
    }

    /**
     * doss中没有asc_code,利用辅助表 dealer_code --> asc_code,给记录打上asc_code;
     *
     * @param filename
     * @return
     * @throws IOException
     */
    public static Map<String, Map<String, String>> loadASC(String filename) throws IOException {
        //InputStream in = Utils.class.getClassLoader().getResourceAsStream(filename);
        InputStream in = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        //Map<String, String> map = new HashMap<>();
        Map<String, Map<String, String>> map = new HashMap<>();

        String s;
        String[] arr;
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            arr = s.split("\\t", -1);
            if (arr.length != 3) {
                continue;
            }
            String version = arr[2];
            if (map.containsKey(version)) {
                map.get(version).put(arr[0], arr[1]);
            } else {
                Map<String, String> mapIn = new HashMap<>();
                mapIn.put(arr[0], arr[1]);
                map.put(version, mapIn);
            }
        }
        reader.close();
        return map.size() == 0 ? null : map;
    }

    public static Map<String, String> loadMapping(String pp) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //System.out.println(conf.get("io.compression.codecs"));
        //指定路径
        Path path = new Path(pp);
        //调用fs的listStatus方法，反悔FileStatus数组
        FileStatus[] fst = fs.listStatus(path);
        Map<String, String> map = new HashMap<String, String>();
        for (FileStatus f : fst) {
            //查看文件的路径
            Path p = f.getPath();
            System.out.println(p.toString());
            if (f.isFile()) {
                FSDataInputStream in = fs.open(p);
                LineIterator li = IOUtils.lineIterator(in, "utf-8");
                while (li.hasNext()) {
                    String s = li.next();
                    if (StringUtils.isBlank(s)) {
                        continue;
                    }
                    String[] arr = s.split("\t", -1);
                    if (arr.length != 4) {
                        continue;
                    }
                    String key = arr[0] + "," + arr[1] + "," + arr[2];
                    map.put(key, arr[3]);
                }
                in.close();
            }

        }
        return map.size() == 0 ? null : map;
    }

    public static void main(String[] args) throws IOException, ParseException {
        String s = "01JAN2017:12:23:12";
        System.out.println(dateTransform(s));
    }
}
