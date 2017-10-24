package com.gm.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by jason on 2017-03-21.
 */
public class MyReduce extends Reducer<Text, IntWritable, Text, NullWritable> {
    protected MultipleOutputs<Text, NullWritable> mos;
    protected String dereplication;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, NullWritable>(context);
        Configuration conf = context.getConfiguration();
        dereplication = conf.get("dereplication");
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if ("yes".equals(dereplication)) {
            int sum = 0;
            for (IntWritable num : values) {
                sum += num.get();
            }

            if (sum > 1) {
                context.getCounter("Kpi-Data", "Duplication").increment(sum - 1);
            }

            context.write(key, NullWritable.get());
        /*String line = key.toString() + "\t" +sum;
        mos.write("error",new Text(line), NullWritable.get(), "error/duplication");*/
        } else if ("no".equals(dereplication)) {
            for (IntWritable i : values) {
                context.write(key, NullWritable.get());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
