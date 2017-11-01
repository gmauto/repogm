package com.jason.model;

import com.jason.util.AMUtil;
import com.jason.util.DimName;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jason on 2017-10-31.
 */
public class DimAbs {
    private String filename;
    //sheetname
    private String sheetName;
    //需要从sheet中取哪些字段
    private String[] index;
    //需要作为key的元素的下标数组
    private int[] keyArr;
    //需要作为val的元素的下标数组
    private int[] valArr;

    public DimAbs() {

    }

    public DimAbs(String filename, String sheetName, String[] index, int[] keyArr, int[] valArr) {
        this.filename = filename;
        this.sheetName = sheetName;
        this.index = index;
        this.keyArr = keyArr;
        this.valArr = valArr;
    }

    public List<String> getRow(Sheet sheet, int row) {
        ArrayList<String> list = new ArrayList<>(10);
        String prefix = "sta-";
        for (String s : index) {
            if (s.startsWith(prefix)) {
                s = s.replaceFirst(prefix, "");
                list.add(s);
            } else {
                int i = Integer.valueOf(s);
                list.add(sheet.getCell(i, row).getContents());
            }
        }
        list.trimToSize();
        return list;
    }

    public String getKey(List<String> list) {
        ArrayList<String> var2 = new ArrayList<>();
        for (int i : keyArr) {
            var2.add(list.get(i));
        }
        return StringUtils.join(var2, "\t");
    }

    public String getVal(List<String> list) {
        ArrayList<String> var2 = new ArrayList<>();
        for (int i : valArr) {
            var2.add(list.get(i));
        }
        return StringUtils.join(var2, "\t");
    }

    public Map<String, String> makeDim(Map<String, String> map) throws IOException, BiffException {
        File xlsFile = new File("DDS数据计算辅助表_1026.xls");
        // 获得工作簿对象
        Workbook workbook = Workbook.getWorkbook(xlsFile);
        // 获得所需要的sheet
        Sheet sheet = workbook.getSheet(sheetName);

        // 遍历工作表
        if (sheet != null) {
            // 获得行数
            int rows = sheet.getRows();
            // 获得列数
            int cols = sheet.getColumns();
            // 读取数据
            for (int row = 1; row < rows; row++) {
                ArrayList<String> list = (ArrayList<String>) getRow(sheet, row);
                map.put(getKey(list), getVal(list));
            }
        }
        workbook.close();
        return map;
    }

}
