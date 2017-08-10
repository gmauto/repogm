package com.gm.label.claimAndFirstMaintenance.main;

import com.gm.label.claimAndFirstMaintenance.map.MyMap;
import com.gm.label.claimAndFirstMaintenance.reduce.MyReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.BZip2Codec;
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
public class Main extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
        //map压缩
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf, "label_claim_maint");
        job.setJarByClass(Main.class);
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReduce.class);
        //job.setNumReduceTasks(conf.getInt("reduces", 500));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "delete", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "error", TextOutputFormat.class, Text.class, Text.class);

        int i = args.length;
        Path[] ps = new Path[i - 1];

        for (int j = 0; j < i - 1; j++) {
            ps[j] = new Path(args[j]);
        }
        FileInputFormat.setInputPaths(job, ps);
        FileOutputFormat.setOutputPath(job, new Path(args[i - 1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Main(), args);
        if (run == 0) {
            System.out.println("Success!");
            System.out.println("outputPath : -------->  " + args[args.length - 1]);
        } else {
            System.out.println("Failure!");
        }
        System.exit(run);
    }
}
