package com.chance.del;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jason on 2017-03-21
 * 该ｒｅｄｕｃｅ程序消耗内存会比较大，所有的结果会被放在ｌｉｓｔ当中.
 */
public class DelReduce extends Reducer<Text, Text, Text, NullWritable> {
    protected MultipleOutputs<Text, NullWritable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] arr = key.toString().split("\\t", -1);
        if (arr.length != 4) {
            return;
        }
        String ascCode = arr[0];
        String orderBalanceDate = arr[1];
        String orderBalanceamount = arr[2];
        String status = arr[3];
        List<String> list = new ArrayList<>();
        for (Text t : values) {
            list.add(t.toString());
        }
        if ("".equals(status)) {
            double amount = Double.valueOf(orderBalanceamount);
            if (0 < amount && amount <= 50 && list.size() >= 10) {
                for (int i = 0; i < list.size(); i++) {
                    String[] array = list.get(i).split("\\001", -1);
                    array[array.length - 1] = "删除3";
                    list.set(i, StringUtils.join(array, "\001"));
                }
            }
        }

        for (String ss : list) {
            context.write(new Text(ss), NullWritable.get());
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
