package com.jason.main;

import com.jason.util.AMUtil;
import jxl.read.biff.BiffException;

import java.io.IOException;

/**
 * Created by jason on 2017-11-01.
 */
public class Main {
    public static void main(String[] args) throws IOException, BiffException {
        String filename = "DDS数据计算辅助表_1104.xls";
        String version = "3";
//        AMUtil.mkEncl(filename,"enclosure",version);
//        AMUtil.mkEgO(filename,"engine_oil",version);
//        AMUtil.mkHFP(filename,"high_flow_parts",version);
//        AMUtil.mkMtc(filename,"maintnance",version);
//        AMUtil.mkFilter(filename,"filter",version);
//        AMUtil.mkMarkOrder(filename,"mark",version);
//        AMUtil.mkMarkDoss(filename,"mark_doss",version);
        AMUtil.mkCity(filename,"city",version);
        AMUtil.mkProvince(filename,"province",version);
        AMUtil.mkPrimaryClassification(filename,"primary_classification",version);
        AMUtil.mkName(filename,"name",version);
//        AMUtil.mkDistributor(filename,"distributor",version);
        AMUtil.mkSexual(filename,"sexual",version);
        AMUtil.mkSecondLevelClassification(filename,"second_level_classification",version);
//        AMUtil.mkCHE(filename,"CHE",version);
//        AMUtil.mkASC_MAPPING(filename,"asc_mapping",version);
//        AMUtil.mkMAPPING(filename,"mapping",version);
//        AMUtil.mk_doss_asc(filename,"doss_asc",version);
    }
}
