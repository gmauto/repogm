package com.jason.util;

/**
 * Created by jason on 2017-10-31.
 */
public class DimName {
    //附件清单
    public static final String SHEET_ENCLOSURE = "附件清单";
    public static final String[] INDEX_ENCLOSURE = {"0", "1", "2", "3", "4"};
    public static final int[] KEY_ENCLOSURE = {0};
    public static final int[] VAL_ENCLOSURE = {1, 2, 3, 4};
    //机油清单
    public static final String SHEET_ENGINE_OIL = "机油清单";
    public static final String[] INDEX_ENGINE_OIL = {"0", "2", "1", "3"};
    public static final int[] KEY_ENGINE_OIL = {0};
    public static final int[] VAL_ENGINE_OIL = {1, 2, 3};
    //高流件
    public static final String SHEET_HFP_LUNT = "高流件-轮胎清单";
    public static final String SHEET_HFP_DIANC = "高流件-蓄电池清单";
    public static final String SHEET_HFP_SCPIAN = "高流件-刹车片";
    public static final String SHEET_HFP_SCPAN = "高流件-刹车盘";
    public static final String SHEET_HFP_HUOH = "高流件-火花塞";
    public static final String SHEET_HFP_QIT = "高流失-其他产品清单";
    public static final String[] INDEX_HFP_LUNT = {"0", "sta-LunT", "3"};
    public static final String[] INDEX_HFP_DIANC = {"0", "sta-DianC", "3"};
    public static final String[] INDEX_HFP_SCPIAN = {"0", "sta-SCPian", "3"};
    public static final String[] INDEX_HFP_SCPAN = {"0", "sta-SCPan", "3"};
    public static final String[] INDEX_HFP_HUOH = {"0", "sta-HuoH", "3"};
    public static final String[] INDEX_HFP_QIT = {"0", "sta-QiT", "3"};
    public static final int[] KEY_HFP = {0};
    public static final int[] VAL_HFP = {1, 2};
    //养护品清单
    public static final String SHEET_MAINTNANCE = "养护品清单";
    public static final String[] INDEX_MAINTNANCE = {"0", "1", "4"};
    public static final int[] KEY_MAINTNANCE = {0};
    public static final int[] VAL_MAINTNANCE = {1, 2};
    //车型对照-order表
    public static final String MARK_ORDER = "车型对照-order表";
    public static final String[] INDEX_MARK_ORDER = {"0", "1", "2", "3", "4"};
    public static final int[] KEY_MARK_ORDER = {0, 1, 2};
    public static final int[] VAL_MARK_ORDER = {3, 4};
    //车型对照-doss表
    public static final String MARK_DOSS = "车型对照-doss表";
    public static final String[] INDEX_MARK_DOSS = {"0", "1", "2", "3"};
    public static final int[] KEY_MARK_DOSS = {0, 1};
    public static final int[] VAL_MARK_DOSS = {2, 3};
    //filter
    public static final String SHEET_FILTER = "机滤清单";
    public static final String[] INDEX_FILTER = {"0", "2", "1", "3"};
    public static final int[] KEY_FILTER = {0};
    public static final int[] VAL_FILTER = {1, 2, 3};

    //city flow表用到的辅助表
    public static final String SHEET_CITY = "city";
    public static final String[] INDEX_CITY = {"0", "1"};
    public static final int[] KEY_CITY = {0};
    public static final int[] VAL_CITY = {1};

    //PROVINCE flow表用到的辅助表
    public static final String SHEET_PROVINCE = "province";
    public static final String[] INDEX_PROVINCE = {"0", "1"};
    public static final int[] KEY_PROVINCE = {0};
    public static final int[] VAL_PROVINCE = {1};

    //primary_classification flow表用到的辅助表
    public static final String SHEET_PRIMARY_CLASSIFICATION = "primary_classification";
    public static final String[] INDEX_PRIMARY_CLASSIFICATION = {"0", "1"};
    public static final int[] KEY_PRIMARY_CLASSIFICATION = {0};
    public static final int[] VAL_PRIMARY_CLASSIFICATION = {1};
    //name flow表用到的辅助表
    public static final String SHEET_NAME = "name";
    public static final String[] INDEX_NAME = {"0", "1"};
    public static final int[] KEY_NAME = {0};
    public static final int[] VAL_NAME = {1};
    //distributor flow表用到的辅助表
    public static final String SHEET_DISTRIBUTOR = "distributor";
    public static final String[] INDEX_DISTRIBUTOR = {"0", "1","2"};
    public static final int[] KEY_DISTRIBUTOR = {0};
    public static final int[] VAL_DISTRIBUTOR = {1,2};
    //sexual flow表用到的辅助表
    public static final String SHEET_SEXUAL = "sexual";
    public static final String[] INDEX_SEXUAL = {"0", "1"};
    public static final int[] KEY_SEXUAL = {0};
    public static final int[] VAL_SEXUAL = {1};
    //second_level_classification flow表用到的辅助表
    public static final String SHEET_SECOND_LEVEL_CLASSIFICATION = "second_level_classification";
    public static final String[] INDEX_SECOND_LEVEL_CLASSIFICATION = {"0", "1"};
    public static final int[] KEY_SECOND_LEVEL_CLASSIFICATION = {0};
    public static final int[] VAL_SECOND_LEVEL_CLASSIFICATION = {1};

}
