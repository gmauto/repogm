package com.gm;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jason on 2017-03-23.
 */
public class CreateTable {
    public static String loadColumn(String filename) throws IOException {
        List<String> list = new ArrayList<String>();
        InputStream in = CreateTable.class.getClassLoader().getResourceAsStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        String s;
        while ((s = reader.readLine()) != null) {
            if (StringUtils.isBlank(s)) {
                continue;
            }
            list.add("  " + s + " string");
        }
        reader.close();
        return StringUtils.join(list, ",\n") + "\n";
    }

    public static void creTable() throws IOException {
        String name = "order";
        String tableName = name;
        String columnName = loadColumn(name);
        String partition = "mon";
        String fieldDelimiter = "\\t";
        String lineDelimiter = "\\n";
        String location = "hdfs://ns1/user/ipsos/private/auto/kpi/" + name + "/";
        System.out.println("function cre_" + name + "(){\n" +
                " hive -e \"\n" +
                "  use ipsos;\n" +
                "  drop table if exists " + tableName + ";\n" +
                "  create external table " + tableName + "(\n" +
                columnName +
                "  )partitioned by(\n" +
                "   " + partition + " string,\n" +
                "   kpi string\n" +
                "  )row format delimited\n" +
                "  fields terminated by '" + fieldDelimiter + "'\n" +
                "  lines terminated by '" + lineDelimiter + "'\n" +
                "  location '" + location + "'\n" +
                " \"\n" +
                "}");
    }

    public static void main(String[] args) throws IOException {
        creTable();
    }
}
