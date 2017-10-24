package com.gm.Util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.Set;

/**
 * Created by jason on 2017-03-22.
 */
public class ForTest {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
        String s;
        Set<String> set = Utils.loadSet("head");
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            Text text = new Text(s);
            //s = new String(text.getBytes(), 0, text.getLength(), "GBK");
        byte[] bb = s.getBytes();
        s = new String(s.getBytes(), "gbk");
        String res = Utils.standardHead(s);
        System.out.println(res);
        System.out.println(Utils.isHead(s, set));
    }
        for (String aa : set) {
        System.out.println(aa);
    }
        reader.close();
    }
}
