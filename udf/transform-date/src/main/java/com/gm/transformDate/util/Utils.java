package com.gm.transformDate.util;

/**
 * Created by jason on 2017-03-29.
 */
public class Utils {
    //日期转换 将表中日期转换为我们要求的日期
    //01JUN2010:00:00:00
    public static String dateTransform(String date) {
        String ymd = date.split(":", 2)[0].toLowerCase();
        if (ymd.length() != 9) {
            return null;
        }
        String dd = ymd.substring(0, 2);
        String yy = ymd.substring(5, 9);
        String mm = ymd.substring(2, 5);
        String newMM = null;
        switch (mm) {
            case "jan":
                newMM = "01";
                break;
            case "feb":
                newMM = "02";
                break;
            case "mar":
                newMM = "03";
                break;
            case "apr":
                newMM = "04";
                break;
            case "may":
                newMM = "05";
                break;
            case "jun":
                newMM = "06";
                break;
            case "jul":
                newMM = "07";
                break;
            case "aug":
                newMM = "08";
                break;
            case "sep":
                newMM = "09";
                break;
            case "oct":
                newMM = "10";
                break;
            case "nov":
                newMM = "11";
                break;
            case "dec":
                newMM = "12";
                break;
            default:
                return null;
        }
        return yy + "-" + newMM + "-" + dd;
    }

}
