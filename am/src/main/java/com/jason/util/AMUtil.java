package com.jason.util;

import com.jason.model.DimAbs;
import jxl.read.biff.BiffException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jason on 2017-10-31.
 */
public class AMUtil {

    //将结果写出到文件
    public static void write(Map<String, String> map, String filename, String version) throws FileNotFoundException, UnsupportedEncodingException {
        String path="C:\\Users\\Administrator\\Desktop\\aa\\";
        //String path = "C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\"+version+"\\"+filename+"\\";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+filename), "utf-8"), true);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            //writer.println(entry.getKey() + "\t" + entry.getValue() + "\t" + version);
            writer.println(entry.getKey() + "\t" + entry.getValue());
        }
        writer.close();
    }

    //将文件用|分割开
    public static void DIMwrite(Map<String, String> map, String filename, String version) throws FileNotFoundException, UnsupportedEncodingException {
        //String path="C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\";
        String path = "C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\"+version+"\\"+filename+"\\";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+filename), "utf-8"), true);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            //writer.println(entry.getKey() + "\t" + entry.getValue() + "\t" + version);
            writer.println(entry.getKey() + "|" + entry.getValue());

        }
        writer.close();
    }

    public static void CHEwrite(Map<String, String> map, String filename, String version) throws FileNotFoundException, UnsupportedEncodingException {
        //String path="C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\";
        String path = "C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\"+version+"\\"+filename+"\\";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+filename), "utf-8"), true);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            writer.println(entry.getKey() + "\t" + version);
//            writer.println(entry.getKey() + "\t" + entry.getValue());
        }
        writer.close();
    }

    //doss_asc
    public static void DOSS_ASCwrite(Map<String, String> map, String filename, String version) throws FileNotFoundException, UnsupportedEncodingException {
        //String path="C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\";
        String path = "C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\"+version+"\\"+filename+"\\";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+filename), "utf-8"), true);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            writer.println(entry.getValue()+"\t"+entry.getKey() + "\t" + version);
//            writer.println(entry.getKey() + "\t" + entry.getValue());
        }
        writer.close();
    }

    public static void ASCMAPPING_write(Map<String, String> map, String filename, String version) throws FileNotFoundException, UnsupportedEncodingException {
        //String path="C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\";
        String path = "C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\"+version+"\\"+filename+"\\";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+filename), "utf-8"), true);
        for (Map.Entry<String, String> entry : map.entrySet()) {
//            writer.println(entry.getKey() + "\t" + version);
            writer.println(entry.getKey() + "\t" + entry.getValue()+ "\t8areas");
            writer.println(entry.getKey() + "\t" + entry.getKey()+ "\tasc");
            writer.println(entry.getKey() + "\t" + 0+ "\twhole_country");
        }
        writer.close();
    }

    //mapping
    public static void MAPPING_write(List<List<String>> list, String filename, String version) throws FileNotFoundException, UnsupportedEncodingException {
        //String path="C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\";
        String path = "C:\\Users\\ww\\Desktop\\utf-8\\simlafile\\"+version+"\\"+filename+"\\";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+filename), "utf-8"), true);
//        for (Map.Entry<String, String> entry : map.entrySet()) {
//            //writer.println(entry.getKey() + "\t" + entry.getValue() + "\t" + version);
//            writer.println(entry.getKey() + "\t" + entry.getValue());
//        }
        for(List<String> ll : list){
            writer.println(ll.get(0)+"\t"+ll.get(1)+"\t"+ll.get(2)+"\t"+ll.get(3));
        }
        writer.close();
    }

    //附件
    public static void mkEncl(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_ENCLOSURE, DimName.INDEX_ENCLOSURE, DimName.KEY_ENCLOSURE, DimName.VAL_ENCLOSURE);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName,version);
    }

    //机油
    public static void mkEgO(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_ENGINE_OIL, DimName.INDEX_ENGINE_OIL, DimName.KEY_ENGINE_OIL, DimName.VAL_ENGINE_OIL);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName,version);
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

        AMUtil.write(map, outName ,version);
    }

    //养护品清单
    public static void mkMtc(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_MAINTNANCE, DimName.INDEX_MAINTNANCE, DimName.KEY_MAINTNANCE, DimName.VAL_MAINTNANCE);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName,version);
    }

    //机滤
    public static void mkFilter(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_FILTER, DimName.INDEX_FILTER, DimName.KEY_FILTER, DimName.VAL_FILTER);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName,version);
    }

    //mark_order
    public static void mkMarkOrder(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_ORDER, DimName.INDEX_MARK_ORDER, DimName.KEY_MARK_ORDER, DimName.VAL_MARK_ORDER);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName ,version);
    }

    //local mark
    public static void mkLocalMark(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.LocalMark, DimName.INDEX_LocalMark, DimName.KEY_LocalMark, DimName.VAL_LocalMark);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.CHEwrite(map, outName ,version);
    }

    //CHE
    public static void mkCHE(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_ORDER, DimName.INDEX_MARK_CHE, DimName.KEY_MARK_CHE, DimName.VAL_MARK_CHE);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.CHEwrite(map, outName,version);
    }

    //asc_mapping
    public static void mkASC_MAPPING(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_ASCMAPPING, DimName.INDEX_ASCMAPPING, DimName.KEY_ASCMAPPING, DimName.VAL_ASCMAPPING);
        Map<String, String> map = new HashMap<>();
//        da.makeDim(map);
        da.makeDim_ASCmapping(map);
        AMUtil.ASCMAPPING_write(map, outName,version);
    }

    //mapping
    public static void mkMAPPING(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_MAPPING, DimName.INDEX_MAPPING, DimName.KEY_MAPPING, DimName.VAL_MAPPING);
//        Map<String, String> map = new HashMap<>();
//        da.makeDim(map);
//        da.makeDim(map);
        List<List<String>> list = new ArrayList<List<String>>();
        da.makeDim_mapping(list);
//        AMUtil.write(map, outName,version);
        AMUtil.MAPPING_write(list,outName,version);
    }

    //mark_doss
    public static void mkMarkDoss(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_DOSS, DimName.INDEX_MARK_DOSS, DimName.KEY_MARK_DOSS, DimName.VAL_MARK_DOSS);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName,version);
    }

    //local mark_doss
    public static void mkLocal_MarkDoss(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_DOSS, DimName.INDEX_MARK_DOSS, DimName.KEY_MARK_DOSS, DimName.VAL_MARK_DOSS);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DOSS_ASCwrite(map, outName,version);
    }

    //doss_asc
    public static void mk_doss_asc(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MARK_DOSS_ASC, DimName.INDEX_DOSS_ASC, DimName.KEY_DOSS_ASC, DimName.VAL_DOSS_ASC);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DOSS_ASCwrite(map, outName,version);
    }

    //local filter
    public static void mk_LocalFilter(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.LOCAL_FILTER, DimName.INDEX_LOCAL_FILTER, DimName.KEY_LOCAL_FILTER, DimName.VAL_LOCAL_FILTER);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.CHEwrite(map, outName,version);
    }
    //local filter
    public static void mk_LocalEngineoil(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.LOCAL_FILTER, DimName.INDEX_LOCAL_FILTER, DimName.KEY_LOCAL_FILTER, DimName.VAL_LOCAL_FILTER);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.CHEwrite(map, outName,version);
    }

    //city flow辅助表
    public static void mkCity(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_CITY, DimName.INDEX_CITY, DimName.KEY_CITY, DimName.VAL_CITY);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DIMwrite(map, outName,version);
    }

    //primary_classification flow辅助表
    public static void mkPrimaryClassification(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_PRIMARY_CLASSIFICATION, DimName.INDEX_PRIMARY_CLASSIFICATION, DimName.KEY_PRIMARY_CLASSIFICATION, DimName.VAL_PRIMARY_CLASSIFICATION);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DIMwrite(map, outName ,version);
    }

    //name flow辅助表
    public static void mkName(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_NAME, DimName.INDEX_NAME, DimName.KEY_NAME, DimName.VAL_NAME);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DIMwrite(map, outName ,version);
    }
    //Maint_Type1 flow辅助表
    public static void mkMaint_Type1(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.MAINT_TYPE1, DimName.INDEX_NAME, DimName.KEY_NAME, DimName.VAL_NAME);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName ,version);
    }

    //Maint_Type1 flow辅助表
    public static void mkType2(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.TYPE2, DimName.INDEX_NAME, DimName.KEY_NAME, DimName.VAL_NAME);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName ,version);
    }

    //distributor flow辅助表
    public static void mkDistributor(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_DISTRIBUTOR, DimName.INDEX_DISTRIBUTOR, DimName.KEY_DISTRIBUTOR, DimName.VAL_DISTRIBUTOR);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.write(map, outName ,version);
    }
    //sexual flow辅助表
    public static void mkSexual(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_SEXUAL, DimName.INDEX_SEXUAL, DimName.KEY_SEXUAL, DimName.VAL_SEXUAL);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DIMwrite(map, outName ,version);
    }
    //province flow辅助表
    public static void mkProvince(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_PROVINCE, DimName.INDEX_PROVINCE, DimName.KEY_PROVINCE, DimName.VAL_PROVINCE);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DIMwrite(map, outName ,version);
    }
    //second_level_classification flow辅助表
    public static void mkSecondLevelClassification(String xlsName, String outName, String version) throws IOException, BiffException {
        DimAbs da = new DimAbs(xlsName, DimName.SHEET_SECOND_LEVEL_CLASSIFICATION, DimName.INDEX_SECOND_LEVEL_CLASSIFICATION, DimName.KEY_SECOND_LEVEL_CLASSIFICATION, DimName.VAL_SECOND_LEVEL_CLASSIFICATION);
        Map<String, String> map = new HashMap<>();
        da.makeDim(map);
        AMUtil.DIMwrite(map, outName ,version);
    }
}
