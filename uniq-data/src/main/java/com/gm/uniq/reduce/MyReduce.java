package com.gm.uniq.reduce;

import com.gm.uniq.Util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

/**
 * Created by jason on 2017-03-21.
 */
public class MyReduce extends Reducer<Text, Text, Text, NullWritable> {
    protected MultipleOutputs<Text, NullWritable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String[]> list = new ArrayList<>();
        for (Text t : values) {
            list.add(t.toString().split("\\001", -1));
        }
        Collections.sort(list, new Comparator<String[]>() {
            @Override
            public int compare(String[] o1, String[] o2) {
                int i1 = Utils.countBlankColumn(o1);
                int i2 = Utils.countBlankColumn(o2);
                if (i1 == i2) {
                    return Arrays.toString(o1).hashCode() - Arrays.toString(o2).hashCode();
                } else {
                    return i1 - i2;
                }

            }
        });
        context.write(new Text(StringUtils.join(list.get(0), "\001")), NullWritable.get());
        for (int i = 1; i < list.size(); i++) {
            //mos.write(new Text(StringUtils.join(list.get(i), "\001")), NullWritable.get(), "delete/delete");
            context.getCounter("Delete lines ", "no ").increment(1L);
        }
        list = null;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (mos != null) {
            mos.close();
        }
    }
}
