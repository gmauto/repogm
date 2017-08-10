
package com.gm.label.others.map;

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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] arr = line.split("\\001", -1);
        int len = arr.length;
        String vin = null;
        if (len == 40) {//order 26
            vin = arr[26];
        } else if (len == 17) {//doss 7
            vin = arr[7];
        } else if (len == 48) {//customer 0
            vin = arr[0];
        }
        if (StringUtils.isBlank(vin)) {
            //mos.write("error", value, new Text(""), "error");
            context.getCounter("Error line", "vin is blank").increment(1L);
        } else {
            context.write(new Text(vin), value);
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
