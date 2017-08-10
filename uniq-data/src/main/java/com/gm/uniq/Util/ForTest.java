package com.gm.uniq.Util;

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
        Collections.sort(list, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });

        System.out.println(list);
    }
}
