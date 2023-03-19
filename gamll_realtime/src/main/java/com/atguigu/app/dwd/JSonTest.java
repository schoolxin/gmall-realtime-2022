package com.atguigu.app.dwd;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
public class JSonTest {
    public static void main(String[] args) {
        String jsons = "{\"common\":{\"ar\":\"440000\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"is_new\":\"0\",\"md\":\"iPhone X\",\"mid\":\"mid_3442864\",\"os\":\"iOS 12.4.1\",\"uid\":\"928\",\"vc\":\"v2.1.134\"},\"page\":{\"during_time\":1957,\"item\":\"19,3,20\",\"item_type\":\"sku_ids\",\"last_page_id\":\"cart\",\"page_id\":\"trade\"},\"ts\":1651303987000}";


        JSONObject jsonObject = JSON.parseObject(jsons);

        System.out.println(jsonObject.getString("common"));
        System.out.println(jsonObject.getJSONObject("common"));
    }
}
