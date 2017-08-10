package com.gm.uniq.Util;

/**
 * Created by jason on 2017-03-21.
 */
public class Utils {
    //获取所给数组中字段为空的个数
    public static int countBlankColumn(String[] arr) {
        int i = 0;
        for (String s : arr) {
            if ("".equals(s)) {
                i++;
            }
        }
        return i;
    }
}
