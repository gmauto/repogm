package com.jason.util;

import com.jason.model.DimAbs;
import jxl.read.biff.BiffException;

import java.io.*;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jason on 2017-10-31.
 */
public class AMUtil {

    //将结果写出到文件
    public static void write(Map<String, String> map, String filename, String version) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filename), "utf-8"), true);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            writer.println(entry.getKey() + "\t" + entry.getValue() + "\t" + version);
        }
        writer.close();
    }

    //附件
    public static void mkEncl(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_ENCLOSURE, DimName.INDEX_ENCLOSURE, DimName.KEY_ENCLOSURE, DimName.VAL_ENCLOSURE);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName+"_"+version,version);
    }

    //机油
    public static void mkEgO(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_ENGINE_OIL, DimName.INDEX_ENGINE_OIL, DimName.KEY_ENGINE_OIL, DimName.VAL_ENGINE_OIL);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName+"_"+version,version);
    }

    //高流件
    public static void mkHFP(String xlsName, String outName, String version) throws IOException, BiffException {
        Map<String, String> map = new HashMap<>();
        //高流件电池
        DimAbs daDianC = new DimAbs(xlsName, DimName.SHEET_HFP_DIANC, DimName.INDEX_HFP_DIANC, DimName.KEY_HFP, DimName.VAL_HFP);
        daDianC.makeDim(map);

        //高流件轮胎
        DimAbs daLunT = new DimAbs(xlsName, DimName.SHEET_HFP_LUNT, DimName.INDEX_HFP_LUNT, DimName.KEY_HFP, DimName.VAL_HFP);
        daLunT.makeDim(map);

        //高流件刹车片
        DimAbs daSCPian = new DimAbs(xlsName, DimName.SHEET_HFP_SCPIAN, DimName.INDEX_HFP_SCPIAN, DimName.KEY_HFP, DimName.VAL_HFP);
        daSCPian.makeDim(map);

        //高流件刹车盘
        DimAbs daSCPan = new DimAbs(xlsName, DimName.SHEET_HFP_SCPAN, DimName.INDEX_HFP_SCPAN, DimName.KEY_HFP, DimName.VAL_HFP);
        daSCPan.makeDim(map);

        //高流件火花塞
        DimAbs daHuoH = new DimAbs(xlsName, DimName.SHEET_HFP_HUOH, DimName.INDEX_HFP_HUOH, DimName.KEY_HFP, DimName.VAL_HFP);
        daHuoH.makeDim(map);

        //高流件其他
        DimAbs daQiT = new DimAbs(xlsName, DimName.SHEET_HFP_QIT, DimName.INDEX_HFP_QIT, DimName.KEY_HFP, DimName.VAL_HFP);
        daQiT.makeDim(map);

        AMUtil.write(map, outName+"_"+version,version);
    }

    //养护品清单
    public static void mkMtc(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_MAINTNANCE, DimName.INDEX_MAINTNANCE, DimName.KEY_MAINTNANCE, DimName.VAL_MAINTNANCE);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName+"_"+version,version);
    }

    //机滤
    public static void mkFilter(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_FILTER, DimName.INDEX_FILTER, DimName.KEY_FILTER, DimName.VAL_FILTER);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName+"_"+version,version);
    }

    //mark_order
    public static void mkMarkOrder(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_ORDER, DimName.INDEX_MARK_ORDER, DimName.KEY_MARK_ORDER, DimName.VAL_MARK_ORDER);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName+"_"+version,version);
    }

    //mark_doss
    public static void mkMarkDoss(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_DOSS, DimName.INDEX_MARK_DOSS, DimName.KEY_MARK_DOSS, DimName.VAL_MARK_DOSS);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName+"_"+version,version);
    }

    public static void main(String[] args) {


    }
}