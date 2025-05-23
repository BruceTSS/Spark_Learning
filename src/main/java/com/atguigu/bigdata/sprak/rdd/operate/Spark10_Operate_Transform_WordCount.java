package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark10_Operate_Transform_WordCount {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> nums = Arrays.asList(1, 2, 3, 4);

        // TODO 分组聚合
        //      1.读取文件
        JavaRDD<String> lineRDD = javaSparkContext.textFile("data/word.txt");
        //      2.将文件数据进行分解
        JavaRDD<String> wordRDD = lineRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //      3.将相同的单词分到一个组中
        //      Hello, Hello, Hello
        //      (Hello, [Hello, Hello, Hello])
        JavaPairRDD<String, Iterable<String>> wordGroupRDD = wordRDD.groupBy(word -> word);
        //      4.计算每个单词的组中的数量即可
        JavaPairRDD<String, Integer> wordCountRDD = wordGroupRDD.mapValues(
                iter -> {
                    int sum = 0;
                    for (String word : iter) {
                        sum += 1;
                    }
                    return sum;
                }
        );
        wordCountRDD.collect().forEach(System.out::println);

        javaSparkContext.close();
    }
}
