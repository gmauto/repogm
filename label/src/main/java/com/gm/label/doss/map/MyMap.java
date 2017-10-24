package com.gm.label.doss.map;

import com.gm.label.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/**
 * 用于给asc打标签  asc_code，一级分类，二级分类
                * 要求进行版本追述，ｄｏｓｓ表的追述字段是partition分区名，
 * Created by jason on 2017-03-21.
                */
        public class MyMap extends Mapper<LongWritable, Text, Text, NullWritable> {
            //protected MultipleOutputs<Text, Text> mos;
            protected Map<String, List<String>> map;
            protected Map<String, Map<String, String>> mapAsc;
            protected Map<String, String> mapping;

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                super.setup(context);
                //mos = new MultipleOutputs<Text, >(context);
                Configuration conf = context.getConfiguration();
                map = Utils.loadMarkClassificationDoss("mark_doss");
                mapAsc = Utils.loadASC("doss_asc");
                mapping = Utils.loadMapping(conf.get("mapping.path"));
            }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                if (StringUtils.isBlank(line)) {
                    return;
                }
                String[] arr = line.split("\\001", -1);
                int len = arr.length;
                //车系
                String marks = null;
                //车型
                String series = null;
                String kk;

        //ch_code
        String dealer_number;//12

        //获取当前读取文件的父目录，用于进行版本追述
        Path path = ((FileSplit) context.getInputSplit()).getPath();
        String partition = path.getParent().getName();


        if (len == 17) {
            marks = arr[14].toUpperCase().trim();
            series = arr[15].toUpperCase().trim();
            dealer_number = arr[12];
            kk = marks + series;
            List<String> list = new ArrayList<>(19);
            Collections.addAll(list, arr);
            //匹配asccode
            String keyAsc = "doss,doss_asc," + partition;
            //给ｄｏｓｓ打asc_code
            Map<String, String> mapa = null;
            boolean boo = false;
            try {
                mapa = mapAsc.get(mapping.get(keyAsc));
                boo = mapa.containsKey(dealer_number);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage() + "\n" + keyAsc + "|||" + dealer_number, e);
            }


            if (boo) {
                list.add(mapa.get(dealer_number));
            } else {
                list.add("");
            }


            String keyClass = "doss,mark_doss," + partition;
            String version = mapping.get(keyClass);
            String keyab = kk + version;
            try {
                version.toString();
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage() + "\n" + keyClass + "|||" + keyab, e);
            }

            //匹配一二级车型
            if (map.containsKey(keyab)) {
                List<String> markClass = map.get(keyab);
                list.add(markClass.get(0));
                list.add(markClass.get(1));
            } else {
                list.add("");
                list.add("");
            }
            context.write(new Text(StringUtils.join(list, "\001")), NullWritable.get());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        /*if (mos != null) {
            mos.close();
        }*/
    }

}
