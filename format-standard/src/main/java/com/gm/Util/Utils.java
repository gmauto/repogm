package com.gm.Util;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by jason on 2017-03-21.
 */
public class Utils {
    public static String standard(String given, String raw_delimiter, String new_delimiter, int len) {
        if (StringUtils.isBlank(given)) {
            return null;
        }
        String[] arr = given.split(raw_delimiter, -1);
        if (arr.length != len) {
            return null;
        }
        arr[0] = StringUtils.removeStart(arr[0], "\"");
        int lastIndex = arr.length - 1;
        arr[lastIndex] = StringUtils.removeEnd(arr[lastIndex], "\"");
        return StringUtils.join(arr, new_delimiter);
    }

    public static String standardHead(String given) {
        if (StringUtils.isBlank(given)) {
            return null;
        }
        String[] arr = given.split("\",\"", -1);

        arr[0] = StringUtils.removeStart(arr[0], "\"");
        int lastIndex = arr.length - 1;
        arr[lastIndex] = StringUtils.removeEnd(arr[lastIndex], "\"");
        return StringUtils.join(arr, "\001");
    }

    public static Set<String> loadSet(String filename) throws IOException {
        InputStream in = Utils.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));

        String s;
        Set<String> set = new HashSet<String>();
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            set.add(s.trim());
        }
        reader.close();
        return set;
    }

    public static boolean isHead(String given, Set<String> set) {
        for (String pre : set) {
            if (given.startsWith(pre)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        String s = "董鸿飞";
        System.out.println(Arrays.toString(s.getBytes()));
        System.out.println(Arrays.toString(s.getBytes("utf-8")));
        System.out.println(Arrays.toString(s.getBytes("gbk")));
        ;
        byte[] bb = {-24, -111, -93, -23, -72, -65, -23, -93, -98};
        String xx;
        System.out.println(xx = new String(bb, "gbk"));
        System.out.println(Arrays.toString(xx.getBytes()));
        System.out.println(Arrays.toString(xx.getBytes("gbk")));
        System.out.println(Arrays.toString(xx.getBytes("utf-8")));


        System.out.println(Charset.defaultCharset().name());

        System.out.println(new String(xx.getBytes("gbk"), "utf-8"));
    }
}
