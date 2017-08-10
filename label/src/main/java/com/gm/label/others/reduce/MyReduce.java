package com.gm.label.others.reduce;

import com.gm.label.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jason on 2017-03-21.
 */
public class MyReduce extends Reducer<Text, Text, Text, NullWritable> {
    protected MultipleOutputs<Text, NullWritable> mos;
    protected Map<String, List<String>> markClass;
    protected Map<String, String> mapping;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs<Text, NullWritable>(context);
        markClass = Utils.loadMarkClassification("mark");

        mapping = Utils.loadMapping(conf.get("mapping.path"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<List<String>> orderList = new ArrayList<>();
        //doss开票日期
        String invoiceDate = null; //10
        //customer 保修开始日期
        String insuranceBeginDate = null;//39
        //customer 建档日期
        String insuranceOrderDate = null;//40
        List<String> invoiceDateList = new ArrayList<>();
        List<String> insuranceBeginDateList = new ArrayList<>();
        List<String> insuranceOrderDateList = new ArrayList<>();
        String line;
        //出库日期
        String outdate = null;
        for (Text t : values) {
            line = t.toString();
            if (StringUtils.isBlank(line)) {
                continue;
            }
            String[] arr = line.split("\\001", -1);
            int len = arr.length;
            if (len == 17) {//doss 7
                invoiceDate = Utils.dateTransform(arr[10]);
                if (invoiceDate != null) {
                    invoiceDateList.add(invoiceDate);
                }

            } else if (len == 48) {//customer 0
                insuranceBeginDate = Utils.dateTransform(arr[7]);//购车日期
                if (insuranceBeginDate != null) {
                    insuranceBeginDateList.add(insuranceBeginDate);
                }
               /* insuranceOrderDate = Utils.dateTransform(arr[40]);
                if (insuranceOrderDate != null) {
                    insuranceOrderDateList.add(insuranceOrderDate);
                }*/
            } else if (len == 40) {
                List<String> ll = new ArrayList<>(50);
                Collections.addAll(ll, arr);
                orderList.add(ll);
            }
        }

        Collections.sort(invoiceDateList);
        Collections.sort(insuranceBeginDateList);
        Collections.sort(insuranceOrderDateList);

        if (invoiceDateList.size() > 0) {
            outdate = invoiceDateList.get(0);
        } else if (insuranceBeginDateList.size() > 0) {
            outdate = insuranceBeginDateList.get(0);
        }/* else if (insuranceOrderDateList.size() > 0) {
            outdate = insuranceOrderDateList.get(0);
        }*/


        //outdate有可能为null；
        String orderBalanceDate = null;//28

        //需要打的标签
        //车龄月数
        String resOutDate;
        //车龄月数
        String numberOfMonth;
        //车龄分类
        String ageType;
        // 一级分类
        String primaryClassification;
        // 二级分类
        String secondLevelClassification;
        // 保内
        String bn;
        // 保外
        String warranty;
        // 车龄分类1(others)
        String ageType2;
        String delete;
        String mark;
        String series;
        String model;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date limitation = null;
        try {
            limitation = df.parse("2013-10-01");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String resBlank = "";
        String resNotKnnow = "未知";

        for (List<String> ls : orderList) {
            delete = ls.get(37);
            if ("删除".equals(delete)) {//如果 维修类别1 是 “删除” 标签 则后面的标签都是 “”
                resOutDate = resBlank;
                numberOfMonth = resBlank;
                ageType = resBlank;
                ageType2 = resBlank;
                bn = resBlank;
                primaryClassification = resBlank;
                secondLevelClassification = resBlank;
            } else {
                orderBalanceDate = Utils.dateTransform(ls.get(28));

                if (outdate == null || orderBalanceDate == null) {
                    resOutDate = resNotKnnow;
                    numberOfMonth = resNotKnnow;
                    ageType = resNotKnnow;
                    ageType2 = resNotKnnow;
                    bn = resNotKnnow;
                } else {
                    resOutDate = outdate;
                    numberOfMonth = Utils.getMonthDiff(orderBalanceDate, outdate);
                    Date dOutDate = null;
                    try {
                        dOutDate = df.parse(outdate);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    List<String> ageList = Utils.getAge(numberOfMonth);
                    ageType = ageList.get(0);
                    ageType2 = ageList.get(1);
                    //出库日期 小于 2013/10/01
                    if (dOutDate.getTime() < limitation.getTime()) {
                        if (Integer.valueOf(numberOfMonth) <= 23) {
                            bn = "保内";
                        } else {
                            bn = "保外";
                        }
                    } else {
                        if (Integer.valueOf(numberOfMonth) <= 35) {
                            bn = "保内";
                        } else {
                            bn = "保外";
                        }
                    }

                }
                mark = ls.get(7).toUpperCase().trim();
                series = ls.get(8).toUpperCase().trim();
                model = ls.get(9).toUpperCase().trim();

                //用来获取mapping表中的分区，
                String dateym = orderBalanceDate.substring(0, 7);

                String version = mapping.get("order,mark," + dateym);
                try {
                    version.toString();
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage() + "\n" + "order,mark," + dateym + "|||", e);
                }

                List<String> ll = markClass.get(mark + series + model + version);

                if (ll == null) {
                    primaryClassification = "未知";
                    secondLevelClassification = "未知";
                } else {
                    primaryClassification = ll.get(0);
                    secondLevelClassification = ll.get(1);
                }
            }

            //添加到list
            ls.add(resOutDate);
            ls.add(String.valueOf(numberOfMonth));
            ls.add(ageType);
            ls.add(ageType2);
            ls.add(bn);
            ls.add(primaryClassification);
            ls.add(secondLevelClassification);
            context.write(new Text(StringUtils.join(ls, "\001")), NullWritable.get());

            resOutDate = null;
            numberOfMonth = null;
            ageType = null;
            ageType2 = null;
            bn = null;
            primaryClassification = null;
            secondLevelClassification = null;
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
