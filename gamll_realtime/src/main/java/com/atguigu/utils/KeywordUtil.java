package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyword(String keywords) throws IOException {

        ArrayList<String> list = new ArrayList<>();

        //创建IK分词对象  ik_smart  ik_max_word
        StringReader reader = new StringReader(keywords);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        Lexeme next = ikSegmenter.next(); //取出切好的分词  每一次的next取一个

        while (next != null) {
            list.add(next.getLexemeText());
            next = ikSegmenter.next();
        }


        return list;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyword("尚硅谷大数据项目之Flink实时数仓"));
    }
}
