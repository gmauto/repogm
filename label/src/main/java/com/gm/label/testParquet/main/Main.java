package com.gm.label.testParquet.main;

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
import org.apache.hadoop.util.ToolRunner;
import parquet.example.data.Group;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.api.DelegatingWriteSupport;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageTypeParser;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jason on 2017-03-21.
 */
public class Main extends Configured implements Tool {
    public static final class MyWriteSupport extends DelegatingWriteSupport<Group> {

        private long count = 0;

        public MyWriteSupport() {
            super(new GroupWriteSupport());
        }

        @Override
        public void write(Group record) {
            super.write(record);
            ++count;
        }

        @Override
        public WriteSupport.FinalizedWriteContext finalizeWrite() {
            Map<String, String> extraMetadata = new HashMap<String, String>();
            extraMetadata.put("my.count", String.valueOf(count));
            return new parquet.hadoop.api.WriteSupport.FinalizedWriteContext(extraMetadata);
        }
    }

    public int run(String[] args) throws Exception {
        String writeSchema = "message example {\n" +
                "required int32 line;\n" +
                "required binary content;\n" +
                "}";
        Configuration conf = getConf();
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
        //map压缩
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
        conf.set("mapping.path", args[0]);
        //conf.set("parquet.write.support.class", GroupWriteSupport.class.getName());
        //conf.set("parquet.example.schema", getMT().getName());
        Job job = Job.getInstance(conf, "label_other");
        job.setJarByClass(Main.class);
        job.setMapperClass(MyMap.class);
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReduce.class);
        //job.setNumReduceTasks(conf.getInt("reduces", 500));
        //job.setOutputKeyClass(Void.class);
        //job.setOutputValueClass(Group.class);
        ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(ParquetOutputFormat.class);
        //job.setOutputFormatClass(AvroParquetOutputFormat.class);

        int i = args.length;
        Path[] ps = new Path[i - 1];

        for (int j = 0; j < i - 1; j++) {
            ps[j] = new Path(args[j]);
        }
        FileInputFormat.setInputPaths(job, ps);
        //FileOutputFormat.setOutputPath(job, new Path(args[i - 1]));
        ParquetOutputFormat.setOutputPath(job, new Path(args[i - 1]));
        ParquetOutputFormat.setWriteSupportClass(job, MyWriteSupport.class);
        GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), conf);
        //ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
        //GroupWriteSupport.setSchema(getMT(), conf);
        //AvroParquetOutputFormat.setSchema(job,StockAvg.SCHEMA$);
        System.out.println(conf.get("parquet.example.schema"));
        System.out.println(GroupWriteSupport.getSchema(conf).getColumns());
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
