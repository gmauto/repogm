package com.gm.transformDate;

import com.gm.transformDate.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by jason on 2017-03-29.
 */
public class TransformDate extends UDF {
    public String evaluate(String rawDate) {
        if (StringUtils.isBlank(rawDate)) {
            return null;
        }
        return Utils.dateTransform(rawDate);
    }
}
