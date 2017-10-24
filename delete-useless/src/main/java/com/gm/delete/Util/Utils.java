package com.gm.delete.Util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jason on 2017-03-21.
 */
public class Utils {
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

    //判断字符串是否可以转化为数值
    public static boolean canTransferToInt(String s) {
        if (StringUtils.isBlank(s)) {
            return false;
        }
        double i;
        try {
            i = Double.valueOf(s);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    //包含雪弗兰  返回 true
    public static boolean isContainCh(Set<String> set, String given) {
        if (StringUtils.isBlank(given)) {
            return false;
        }
        for (String s : set) {
            if (given.equals(s)) {
                return true;
            }
        }
        return false;
    }

    //加载雪弗兰品牌
    public static Map<String, Set<String>> loadCH(String filename) throws IOException {
        //InputStream in = Utils.class.getClassLoader().getResourceAsStream(filename);
        InputStream in = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        //FileReader fr = new FileReader(filename);
        //BufferedReader reader = new BufferedReader(fr);
        //用于存放 CHE 信息 key 是版本号 ，value 为品牌，车系，车型
        Map<String, Set<String>> map = new HashMap<>();
        String s;
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            String[] arr = s.split("\t", -1);
            if (arr.length != 4) {
                continue;
            }
            //set.add(arr[0].toUpperCase().trim()+arr[1].toUpperCase().trim()+arr[2].toUpperCase().trim());
            if (map.containsKey(arr[3])) {
                map.get(arr[3]).add(arr[0].toUpperCase().trim() + arr[1].toUpperCase().trim() + arr[2].toUpperCase().trim());
            } else {
                Set<String> set = new HashSet<>();
                set.add(arr[0].toUpperCase().trim() + arr[1].toUpperCase().trim() + arr[2].toUpperCase().trim());
                map.put(arr[3], set);
            }
        }
        reader.close();
        return map;
    }

    //遍历集群pp目录下的文件加载到map当中，key [arr[0],arr[1],arr[2]] value 位 arr[3]
    //这个方法在调用时容易忘记传mapping 路径，所以在结尾 map 大小为0 时抛出异常
    public static Map<String, String> loadMapping(String pp) throws Exception {
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
        if (map.size() == 0) {
            throw new RuntimeException("mapping 路径可能不存在");
        } else {
            return map;
        }

    }


    public static void main(String[] args) throws IOException {
        //loadCH("conf/CHEv1");
        System.out.println(dateTransform("2017-01-01 00:00:00:0"));
        String regex = "\\d{4}-\\d{2}-\\d{2}.*";
        String s = "2017-03-04";
        System.out.println(s.matches(regex));
    }


}
