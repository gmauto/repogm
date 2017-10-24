package com.jason;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jason on 2017-06-20.
 */
public class Combine {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //System.out.println(conf.get("io.compression.codecs"));
        //指定路径
        Path path = new Path(args[0]);
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
        for (String s : map.keySet()) {
            System.out.println(s + "\t" + map.get(s));
        }
    }

}
