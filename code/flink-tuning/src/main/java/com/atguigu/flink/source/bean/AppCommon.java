package com.atguigu.flink.source.bean;

import com.atguigu.flink.source.config.AppConfig;
import com.atguigu.flink.source.util.ParamUtil;
import com.atguigu.flink.source.util.RanOpt;
import com.atguigu.flink.source.util.RandomNum;
import com.atguigu.flink.source.util.RandomOptionGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder(builderClassName = "Builder")
public class AppCommon {

    private String mid; // (String) 设备唯一标识
    private String uid; // (String) 用户uid
    private String vc;  // (String) versionCode，程序版本号
    private String ch;  // (String) 渠道号，应用从哪个渠道来的。
    private String os;  // (String) 系统版本
    private String ar;  // (String) 区域
    private String md;  // (String) 手机型号
    private String ba;  // (String) 手机品牌
    private String is_new; // 是否新用户

    public static AppCommon build() {
        String mid; // (String) 设备唯一标识
        String uid; // (String) 用户uid
        String vc;  // (String) versionCode，程序版本号
        String ch;  // (String) 渠道号，应用从哪个渠道来的。
        String os;  // (String) 系统版本
        String ar;  // (String) 区域
        String md;  // (String) 手机型号
        String ba;  // (String) 手机品牌
        String isnew;

        Boolean isSkew = ParamUtil.checkBoolean(AppConfig.mock_skew);
        RandomOptionGroup isSkewRandom = RandomOptionGroup.builder().add(true, 80).add(false, 20).build();
        // 设备唯一标识
        // 添加倾斜开关
        if (isSkew && isSkewRandom.getRandBoolValue()) {
            mid = "mid_" + AppConfig.max_mid / 2 + "";
        } else {
            mid = "mid_" + RandomNum.getRandInt(1, AppConfig.max_mid) + "";
        }


        // 区域
        ar = new RandomOptionGroup<String>(new RanOpt<String>("110000", 10),
                new RanOpt<String>("310000", 10),
                new RanOpt<String>("230000", 10),
                new RanOpt<String>("370000", 10),
                new RanOpt<String>("420000", 10),
                new RanOpt<String>("440000", 10),
                new RanOpt<String>("500000", 10),
                new RanOpt<String>("530000", 10)
        ).getRandStringValue();

        // 手机型号
        md = new RandomOptionGroup<String>(new RanOpt<String>("Xiaomi 9", 30),
                new RanOpt<String>("Xiaomi 10 Pro ", 30),
                new RanOpt<String>("Xiaomi Mix2 ", 30),
                new RanOpt<String>("iPhone X", 20),
                new RanOpt<String>("iPhone 8", 20),
                new RanOpt<String>("iPhone Xs", 20),
                new RanOpt<String>("iPhone Xs Max", 20),
                new RanOpt<String>("Huawei P30", 10),
                new RanOpt<String>("Huawei Mate 30", 10),
                new RanOpt<String>("Redmi k30", 10),
                new RanOpt<String>("Honor 20s", 5),
                new RanOpt<String>("vivo iqoo3", 20),
                new RanOpt<String>("Oneplus 7", 5),
                new RanOpt<String>("Sumsung Galaxy S20", 3)).getRandStringValue();

        // 手机品牌
        ba = md.split(" ")[0];

        if (ba.equals("iPhone")) {
            // 渠道号
            ch = "Appstore";
            os = "iOS " + new RandomOptionGroup<String>(new RanOpt<String>("13.3.1", 30),
                    new RanOpt<String>("13.2.9", 10),
                    new RanOpt<String>("13.2.3", 10),
                    new RanOpt<String>("12.4.1", 5)
            ).getRandStringValue();

        } else {
            // 渠道号
            ch = new RandomOptionGroup<String>(new RanOpt<String>("xiaomi", 30),
                    new RanOpt<String>("wandoujia", 10),
                    new RanOpt<String>("web", 10),
                    new RanOpt<String>("huawei", 5),
                    new RanOpt<String>("oppo", 20),
                    new RanOpt<String>("vivo", 5),
                    new RanOpt<String>("360", 5)
            ).getRandStringValue();
            os = "Android " + new RandomOptionGroup<String>(new RanOpt<String>("11.0", 70),
                    new RanOpt<String>("10.0", 20),
                    new RanOpt<String>("9.0", 5),
                    new RanOpt<String>("8.1", 5)
            ).getRandStringValue();
        }

        // 程序版本号
        vc = "v" + new RandomOptionGroup<String>(new RanOpt<String>("2.1.134", 70),
                new RanOpt<String>("2.1.132", 20),
                new RanOpt<String>("2.1.111", 5),
                new RanOpt<String>("2.0.1", 5)
        ).getRandStringValue();

        uid = RandomNum.getRandInt(1, AppConfig.max_uid) + "";

        isnew = RandomNum.getRandInt(0, 1) + "";

        AppCommon appBase = new AppCommon(mid, uid, vc, ch, os, ar, md, ba, isnew);
        return appBase;
    }


}
