
package com.gm.label.testParquet.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.ContextUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * Created by jason on 2017-03-21.
 */
public class MyMap extends Mapper<LongWritable, Text, Void, Group> {
    protected MultipleOutputs<Text, Text> mos;
    protected Set<String> set;
    private SimpleGroupFactory factory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
        Configuration conf = context.getConfiguration();
        String s1 = conf.get("parquet.example.schema");
        String s2 = GroupWriteSupport.getSchema(conf).getColumns().toString();

        throw new IOException(s1 + "  ," + s2);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] arr = line.split("\t", -1);
        int len = arr.length;
        String vin = null;
        if (len >= 3) {//order 26
            Group group = factory.newGroup()
                    .append("line", (int) key.get())
                    .append("content", arr[1]);
            context.write(null, group);
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
