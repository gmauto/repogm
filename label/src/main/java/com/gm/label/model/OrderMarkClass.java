package com.gm.label.model;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jason on 2017-06-21.
 */
public class OrderMarkClass {
    //包含车辆的mark， series， model
    private String motorInfo;
    private List<String> classList;
    private String version;

    public OrderMarkClass() {

    }

    public OrderMarkClass(String mark, String series, String model, String classA, String classB, String version) {
        classList = new ArrayList<>();
        motorInfo = mark.toUpperCase() + series.toUpperCase() + model.toUpperCase();
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

    public static OrderMarkClass parseOMC(String[] arr) {
        if (StringUtils.isBlank(arr[3]) || StringUtils.isBlank(arr[4])) {
            return null;
        }
        return new OrderMarkClass(arr[0], arr[1], arr[2], arr[3], arr[4], arr[5]);
    }
}
