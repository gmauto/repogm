package com.gm.map;

import com.gm.Util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Set;

/**
 * Created by jason on 2017-03-21.
 */
public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    protected String raw_delimiter;
    protected String new_delimiter;
    protected MultipleOutputs<Text, IntWritable> mos;
    protected int len;
    protected Set<String> set;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, IntWritable>(context);
        Configuration conf = context.getConfiguration();
        len = conf.getInt("len", 10);
        raw_delimiter = conf.get("raw-delimiter");
        new_delimiter = "\001";
        set = Utils.loadSet("head");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line;
        try {
            line = new String(value.getBytes(), 0, value.getLength(), "GBK");
            //line = value.toString();
        } catch (Exception e) {
            return;
        }

        if (StringUtils.isBlank(line)) {
            return;
        }

       /* if (set.contains(line)) {
            mos.write("error", new Text(line), NullWritable.get(), "error/head");
            return;
        }*/

        String res = Utils.standard(line, raw_delimiter, new_delimiter, len);

        if (res == null) {
            context.getCounter("Kpi-Data", "Len error").increment(1l);
            //mos.write(new Text(line), NullWritable.get(), "error/error");
        } else if (Utils.isHead(res, set)) {
            context.getCounter("Kpi-Data", "Head line").increment(1l);
            //mos.write(new Text(res), NullWritable.get(), "error/head");
        } else {
            context.write(new Text(res), new IntWritable(1));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (mos != null) {
            mos.close();
        }
    }

}
