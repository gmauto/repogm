package com.gm.label.model;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jason on 2017-06-22.
 */
public class DossMarkClass {
    private String motorInfo;
    private List<String> classList;
    private String version;

    public DossMarkClass() {

    }

    public DossMarkClass(String marks, String series, String classA, String classB, String version) {
        motorInfo = marks.toUpperCase() + series.toUpperCase();
        classList = new ArrayList<>();
        classList.add(classA);
        classList.add(classB);
        this.version = version;
    }

    public void setMotorInfo(String motorInfo) {
        this.motorInfo = motorInfo;
    }

    public void setClassList(List<String> classList) {
        this.classList = classList;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getMotorInfo() {
        return motorInfo;
    }

    public List<String> getClassList() {
        return classList;
    }

    public String getVersion() {
        return version;
    }

    public static DossMarkClass parseDMC(String[] arr) {
        if (StringUtils.isBlank(arr[2]) || StringUtils.isBlank(arr[3])) {
            return null;
        }
        return new DossMarkClass(arr[0], arr[1], arr[2], arr[3], arr[4]);
    }
}
