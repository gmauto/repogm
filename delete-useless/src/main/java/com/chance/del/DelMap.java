package com.chance.del;

import com.gm.delete.Util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by jason on 2017-03-21.
 */
public class DelMap extends Mapper<LongWritable, Text, Text, Text> {
    protected MultipleOutputs<Text, Text> mos;
    //加载chev表
    protected Map<String, Set<String>> mapChev;
    /*加载集群mapping表
    key为 “order,mark,2015-10”                value 为 “1”
          fact_table	dim_ta ble	month	           version*/

    protected Map<String, String> mapping;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, Text>(context);
        Configuration conf = context.getConfiguration();
        //set = Utils.loadCH("CHE");
        mapChev = Utils.loadCH("CHE");
        //
        try {
            mapping = Utils.loadMapping(conf.get("mapping.path"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String status1 = "删除1";
        String status2 = "删除2";
        String status3 = "删除3";
        String status = "";
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] arr = line.split("\\001", -1);
        /*判断车型是否存在，若存在不为雪弗兰则状态为  删除 判断品牌需要三个字段，品牌，车系，车型，这三个字段的组合如果包含在
        set中则说明为雪弗兰，否则不为雪弗兰*/
        //品牌
        String marks = arr[7].toUpperCase().trim();
        //车系
        String series = arr[8].toUpperCase().trim();
        //车型
        String model = arr[9].toUpperCase().trim();
        //组合后的品牌
        String markNew = marks + series + model;
        //维修类别
        String maintDesc = arr[32];
        //出厂里程
        String mileage = arr[12];
        //结算金额
        String orderBalanceamount = arr[30];
        //结算日期
        String orderBalanceDate = arr[28];
        //4s店编号
        String ascCode = arr[27];
        orderBalanceDate = Utils.dateTransform(orderBalanceDate);
        //需要判断是否为雪佛兰，需要用到CHE文件，不同的日期需要匹配不同的CHE版本号
        if (orderBalanceDate == null) {
            context.getCounter("Error line", "error orderBalanceDate").increment(1L);
            return;
        }
        String dateym = orderBalanceDate.substring(0, 7);
        String cheKey = "order,CHE," + dateym;


        //维修类型
        String maintType = arr[35];



        /*20170504添加新代码，
        把label_order中  MAINT_TYPE 为1010 且 MAINT_DESC=''  修改成“事故车(无积分)”
        MAINT_TYPE 为1011 且 MAINT_DESC=''  修改成“事故车(非保险)”*/
        if ("".equals(maintDesc) && "1010".equals(maintType)) {
            maintDesc = "事故车(无积分)";
            arr[32] = "事故车(无积分)";
        } else if ("".equals(maintDesc) && "1011".equals(maintType)) {
            maintDesc = "事故车(非保险)";
            arr[32] = "事故车(非保险)";
        }

        //如果4s店的code不存在则 状态为删除
        if (StringUtils.isBlank(ascCode)) {
            status = status1;
        } else {
            /*1删除非雪佛兰   品牌  的车型：将“品牌”列为类似于QT的工单删除*/
            try {
                boolean boo = Utils.isContainCh(mapChev.get(mapping.get(cheKey)), markNew);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage() + "\n" + cheKey + "|||" + markNew, e);
            }
            if (StringUtils.isBlank(marks) || StringUtils.isBlank(series) || StringUtils.isBlank(model) || !Utils.isContainCh(mapChev.get(mapping.get(cheKey)), markNew)) {//判断是否为雪弗兰需要重新写方法
                status = status1;
            } else {
                //2只保留售后部分的工单：将      “维修类别”     为"延保”"空值”和     出厂里程    小于等于200的    “装潢”    删除，如果维修类为 SQWX 也打删除标签，
                if (StringUtils.isBlank(maintDesc) || maintDesc.contains("延保") || (maintDesc.contains("装潢") && Utils.canTransferToInt(mileage) && Double.valueOf(mileage) <= 200) || "SQWX".equals(maintType)) {
                    status = status2;
                } else {
                    //3假单处理：同一个4s店  工单“结算金额”满足  当天   有  10   张及以上重复金额，且金额大于     0小于等于50    元的工单，判定为*/
                    if (!Utils.canTransferToInt(orderBalanceamount) || orderBalanceDate == null) {
                        status = status3;
                    }
                }
            }
        }
        String[] array = {ascCode, orderBalanceDate, orderBalanceamount, status};
        line = StringUtils.join(arr, "\001");
        context.write(new Text(StringUtils.join(array, "\t")), new Text(line + "\001" + status));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (mos != null) {
            mos.close();
        }
    }

}
