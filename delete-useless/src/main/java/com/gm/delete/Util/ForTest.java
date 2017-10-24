package com.gm.delete.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by jason on 2017-03-22.
 */
public class ForTest {
    public static void main(String[] args) throws IOException {
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(2);
        System.out.println(list);
        list.set(0, 10);
        System.out.println(Utils.dateTransform("2017-09-09 00:00:00.)"));
    }
}
