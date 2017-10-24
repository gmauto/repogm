package com.gm.main;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Created by jason on 2017-03-21.
 */
public class Main {
    public static void main(String[] args) throws UnsupportedEncodingException {
       /* String s = "2200004\",\"\",\"24405740\",\"前&后侧门门槛饰板固定件总成 *不带面漆\",\"2.4000\",\"24.00\",\"2.81\",\"0010\",\"0033\",\"\",\"324363795\",\"\",\"2012-08-08\",\"28SEP2012:00:00:00\",\"28SEP2012:00:00:00\",\"ARO12080364\",\"ABO12091562\",\"0074\",\"维修\",\"BXSG\",\"保险钣喷\",\"辽DX4444\",\"2G1F91E36C9133051\",\"CHEVROLET\",\"CAMARO\",\"CAMARO 3.6\",\"28.1";
        String aa;
        System.out.println(aa = Utils.standard(s,"\",\"", "\001", 27));
        System.out.println(aa.split("\\u0001" + "", -1).length);*/
        String s = "索赔单号";
        byte[] bb = s.getBytes();
        System.out.println(Arrays.toString(bb));
        String s2 = new String(s.getBytes(), 0, bb.length, "utf-8");
        System.out.println(s2);
        System.out.println(s2.length());
    }
}
