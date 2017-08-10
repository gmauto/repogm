package com.gm.label.testParquet.test;

import com.gm.label.testParquet.map.MyMap;
import com.gm.label.testParquet.reduce.MyReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Created by jason on 2017-07-07.
 */
public class Foo {
    public static void main(String[] args) {
        String writeSchema = "message example {\n" +
                "required int32 line;\n" +
                "required binary content;\n" +
                "}";
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), conf);
        System.out.println(conf.get("parquet.example.schema"));
        System.out.println(GroupWriteSupport.getSchema(conf).getColumns());
    }

}
