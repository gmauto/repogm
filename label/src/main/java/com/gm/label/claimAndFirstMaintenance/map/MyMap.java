package com.gm.label.claimAndFirstMaintenance.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Set;

/**
 * Created by jason on 2017-03-21.
 */
public class MyMap extends Mapper<LongWritable, Text, Text, Text> {
    protected MultipleOutputs<Text, Text> mos;
    protected Set<String> set;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, Text>(context);
        Configuration conf = context.getConfiguration();
    }

    @Override
    /*是否首保	是否索赔
    需要关联claim表 索赔单号 4s
    ！注意：order表已经在最后一列打入一列“删除”标签，字段数 +1*/
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] arr = line.split("\\001", -1);
        int len = arr.length;
        //4s店
        String ascCode = null;
        //索赔单号
        String claimOrderNumber = null;

        if (len == 20) {//如果字段数是20则为claim表，
            ascCode = arr[19];
            claimOrderNumber = arr[2];
        } else if (len == 38) {//如果字段数十38，则为order表
            ascCode = arr[27];
            claimOrderNumber = arr[24];
        }

        /*if (StringUtils.isBlank(ascCode) || StringUtils.isBlank(claimOrderNumber)) {
            mos.write("error", value, new Text(""), "error/error");
        } else {*/
        String[] keys = {ascCode, claimOrderNumber};
        context.write(new Text(StringUtils.join(keys, "\t")), value);
 /*       }*/


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (mos != null) {
            mos.close();
        }
    }

}
