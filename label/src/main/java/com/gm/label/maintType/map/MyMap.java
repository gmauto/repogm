package com.gm.label.maintType.map;

import com.gm.label.util.Utils;
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
        //4s店
        String ascCode = null;
        //工单号
        String orderNumber = null;
        //vin
        String vin = null;
        //结算日期
        String orderBalanceDate = null;

        String[] arr = line.split("\\001", -1);
        int len = arr.length;
        //order表
        if (len == 40) {
            ascCode = arr[27];
            //orderNumber = arr[3];
            vin = arr[26];
            orderBalanceDate = Utils.dateTransform(arr[28]);

        } else if (len == 27) {//part表
            ascCode = arr[0];
            //orderNumber = arr[15];
            vin = arr[22];
            orderBalanceDate = Utils.dateTransform(arr[13]);
        }
        if (StringUtils.isBlank(ascCode) || StringUtils.isBlank(vin) || StringUtils.isBlank(orderBalanceDate)) {
            //mos.write("error", value, new Text(""), "error/error");
            context.getCounter("Error line", "asc or vin or orderbalancedate is null").increment(1L);
        } else {
            String[] aa = {ascCode, vin, orderBalanceDate};
            context.write(new Text(StringUtils.join(aa, "\t")), value);
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
