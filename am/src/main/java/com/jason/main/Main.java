package com.jason.main;

import com.jason.util.AMUtil;
import jxl.read.biff.BiffException;

import java.io.IOException;

/**
 * Created by jason on 2017-11-01.
 */
public class Main {
    public static void main(String[] args) throws IOException, BiffException {
        String filename = "";
        String version = "3";
        AMUtil.mkEncl(filename,"enclosure",version);
        AMUtil.mkEgO(filename,"engine_oil",version);
        AMUtil.mkHFP(filename,"high_flow_parts",version);
        AMUtil.mkMtc(filename,"maintnance",version);
        AMUtil.mkFilter(filename,"filter",version);
        AMUtil.mkMarkOrder(filename,"mark",version);
        AMUtil.mkMarkDoss(filename,"mark_doss",version);
    }
}
