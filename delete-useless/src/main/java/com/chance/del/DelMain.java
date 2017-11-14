package com.chance.del;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by jason on 2017-03-21.
 */
public class DelMain extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
        //map压缩
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
        conf.set("mapping.path", args[0]);
        Job job = Job.getInstance(conf, "uniq");
        job.setJarByClass(DelMain.class);
        job.setMapperClass(DelMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DelReduce.class);
        //job.setNumReduceTasks(conf.getInt("reduces", 500));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "delete", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "error", TextOutputFormat.class, Text.class, Text.class);

        int i = args.length;
        Path[] ps = new Path[i - 2];

        for (int j = 1; j < i - 1; j++) {
            ps[j - 1] = new Path(args[j]);
        }
        FileInputFormat.setInputPaths(job, ps);
        FileOutputFormat.setOutputPath(job, new Path(args[i - 1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new DelMain(), args);
        if (run == 0) {
            System.out.println("Success!");
            System.out.println("outputPath : -------->  " + args[args.length - 1]);
        } else {
            System.out.println("Failure!");
        }
        System.exit(run);
    }
}
