package com.gm.transformDate;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * Created by jason on 2017-04-26.
 */
public class IsNull extends UDF {
    public int evaluate(String line) {
        if (StringUtils.isBlank(line)) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) {
        System.out.println(new IsNull().evaluate("4"));
    }
}
