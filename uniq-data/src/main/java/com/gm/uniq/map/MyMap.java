package com.gm.uniq.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jason on 2017-03-21.
 */
public class MyMap extends Mapper<LongWritable, Text, Text, Text> {
    protected MultipleOutputs<Text, Text> mos;
    protected int[] index;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, Text>(context);
        Configuration conf = context.getConfiguration();
        String ind = conf.get("index");
        String[] arr = ind.split(",", -1);
        index = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            index[i] = Integer.valueOf(arr[i]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] arr = line.split("\\001", -1);
        if (index[index.length - 1] >= arr.length) {
            return;
        }
        ArrayList<String> list = new ArrayList<>();
        for (int i : index) {
            list.add(arr[i]);
        }
        context.write(new Text(StringUtils.join(list, ",")), value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (mos != null) {
            mos.close();
        }
    }

}
