package com.gm.transformDate;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by jason on 2017-04-08.
 */
public class MonDiff extends UDF {
    public String evaluate(String big, String small) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date dBig = null;
        Date dSmall = null;
        Calendar cBig = Calendar.getInstance();
        Calendar cSmall = Calendar.getInstance();
        try {
            dBig = df.parse(big);
            dSmall = df.parse(small);
            cBig.setTime(dBig);
            cSmall.setTime(dSmall);
            int yearBig = cBig.get(Calendar.YEAR);
            int yearSmall = cSmall.get(Calendar.YEAR);
            int monBig = cBig.get(Calendar.MONTH);
            int monSmall = cSmall.get(Calendar.MONTH);
            return String.valueOf((yearBig - yearSmall) * 12 + (monBig - monSmall));
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return "未知";
    }

}
