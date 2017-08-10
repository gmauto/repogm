package com.gm.label.maintType.reduce;

import com.gm.label.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
    //protected Map<String, Set<String>> filterAndEngineOil;

    //filter
    protected Map<String, Set<String>> mapFilter;
    //engineOil
    protected Map<String, Set<String>> mapEngineOil;
    //存放 mapping表的数据
    protected Map<String, String> mapping;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, NullWritable>(context);
        //filterAndEngineOil = Utils.loadMap("part");
        Configuration conf = context.getConfiguration();
        mapFilter = Utils.loadMap("filter");
        mapEngineOil = Utils.loadMap("engineoil");
        mapping = Utils.loadMapping(conf.get("mapping.path"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //存储工单号对应的零件，零件放在set中
        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        //存储order数据
        List<List<String>> orderList = new ArrayList<List<String>>();
        String filter = "filter";
        String engineOil = "engineOil";
        String line;
        String[] arr;
        String partNumber;
        String orderNumber;
        String part;
        boolean isFilter = false;

        //根据不同的时间匹配 不同的 filter engineoil 版本号
        String orderbalancedate = key.toString().split("\t", -1)[2];
        String dateym = orderbalancedate.substring(0, 7);

        String keyF = "order,filter," + dateym;
        String keyE = "order,engineoil," + dateym;


        for (Text t : values) {
            line = t.toString();
            arr = line.split("\\001", -1);
            int len = arr.length;

            if (len == 40) {//order
                List<String> list = new ArrayList<String>();
                Collections.addAll(list, arr);
                orderList.add(list);
            } else if (len == 27) {//part
                //将part表中工单 对应的 零件 存储起来
                orderNumber = arr[15];
                partNumber = arr[2].trim().toLowerCase();
                boolean boo = false;
                boolean boo2 = false;
                try {
                    boo = mapFilter.get(mapping.get(keyF)).contains(partNumber);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage() + "\n" + keyF + "|||" + partNumber, e);
                }
                try {
                    boo2 = mapEngineOil.get(mapping.get(keyE)).contains(partNumber);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage() + "\n" + keyE + "|||" + partNumber, e);
                }

                if (boo) {
                    part = filter;
                    isFilter = true;
                } else if (boo2) {
                    part = engineOil;
                } else {
                    part = "other";
                }
                if (StringUtils.isNotBlank(orderNumber)) {
                    if (map.containsKey(orderNumber)) {
                        map.get(orderNumber).add(part);
                    } else {
                        Set<String> set = new HashSet<String>();
                        set.add(part);
                        map.put(orderNumber, set);
                    }
                }
            }
        }

        //维修类别 desc
        String maintDesc;//32
        //维修类别1 第38列
        String maintType1;//37
        //工单号  3
        //是否索赔
        String claim;
        String accident = "事故";
        String decoration = "装潢";
        String maintain = "保养";
        String fix = "维修";
        String other = "其他";
        for (List<String> l : orderList) {
            maintType1 = l.get(37);
            maintDesc = l.get(32);
            orderNumber = l.get(3);
            claim = l.get(39);
            if (!"".equals(maintType1)) {
                continue;
            }
            if (maintDesc.contains("事故") || maintDesc.contains("钣") || maintDesc.contains("喷") || maintDesc.contains("保险")) {
                maintType1 = accident;
            } else if (maintDesc.contains("装潢")) {
                maintType1 = decoration;
            } else {
                Set<String> pp = map.get(orderNumber);
                if (pp == null) {
                    if ("".equals(claim)) {
                        maintType1 = fix;
                    } else {
                        maintType1 = other;
                    }
                } else {
                    if (pp.contains(filter)) {
                        maintType1 = maintain;
                    } else if (pp.contains(engineOil) && isFilter) {
                        maintType1 = maintain;
                    } else {
                        if ("".equals(claim)) {
                            maintType1 = fix;
                        } else {
                            maintType1 = other;
                        }
                    }
                }
            }
            l.set(37, maintType1);
        }

        for (List<String> l : orderList) {
            context.write(new Text(StringUtils.join(l, "\001")), NullWritable.get());
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
