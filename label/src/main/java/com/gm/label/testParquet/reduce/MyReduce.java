package com.gm.label.testParquet.reduce;

import com.gm.label.testParquet.main.Main;
import com.gm.label.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jason on 2017-03-21.
 */
public class MyReduce extends Reducer<Text, Text, NullWritable, Group> {
    private SimpleGroupFactory f;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //f = new SimpleGroupFactory(Main.getMT());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t : values) {
            String[] arr = t.toString().split("\t");
            if (arr.length >= 2) {
                Group group = f.newGroup().append("cf:name", arr[0]).append("cf:age", arr[1]);
                try {
                    context.write(NullWritable.get(), group);
                } catch (Exception e) {
                    throw new IOException("-----------" + e.toString());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
