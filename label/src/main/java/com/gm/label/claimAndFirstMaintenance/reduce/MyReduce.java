package com.gm.label.claimAndFirstMaintenance.reduce;

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
        List<List<String>> orderList = new ArrayList<List<String>>();

        //首保标致  是首保则firstMaintenance会被设置为 1 ，否则为 “” 空字符串。
        String firstMaintenance = "";
        //索赔标致 如果是索赔则 claim的值被设置为1，否则为 “” 空字符串。
        String claim = "";

        //操作代码
        String operationCode; //11
        //审核结果
        String claimResult; //4

        //索赔类型
        String claimType;//5

        for (Text t : values) {
            String[] arr = t.toString().split("\\001", -1);
            int i = arr.length;
            if (i == 20) {
                operationCode = arr[11].trim();
                claimResult = arr[4].trim();

                claimType = arr[5].trim();
                //［索赔申请结果查询］中操作代码”为Z0062，且“审核结果”显示为“审核同意/33”的索赔单， 用“索赔单号”匹配到［维修业务查询］中的工单，为首保工
                if ("Z0062".equals(operationCode) && (claimResult.contains("审核同意") || "33".equals(claimResult))) {
                    firstMaintenance = "1";
                }
                //［索赔申请结果查询］中的非促销（“索赔类型”为ZSSP）索赔单数（不管是否审核同意或审核同意后抵扣），用“索赔单号”匹配到［维修业务查询］中的工
                if (!"ZSSP".equals(claimType)) {
                    claim = "1";
                }
            } else if (i == 38) {
                List<String> list = new ArrayList<String>(40);
                Collections.addAll(list, arr);
                orderList.add(list);
            }
        }

        String keyStirng = key.toString();
        //ascCode, claimOrderNumber

        //记住要过滤掉为删除的记录
        for (List<String> l : orderList) {
            //如果维修类别1 为“” 则添加 是否首保和
            if ("".equals(l.get(37))) {
                l.add(firstMaintenance);
                l.add(claim);
            } else {
                l.add("");
                l.add("");
            }
            context.write(new Text(StringUtils.join(l, "\001")), NullWritable.get());
        }
        orderList = null;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (mos != null) {
            mos.close();
        }
    }
}

