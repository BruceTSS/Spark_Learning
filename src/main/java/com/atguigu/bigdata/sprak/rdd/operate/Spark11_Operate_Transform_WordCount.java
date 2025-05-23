package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark11_Operate_Transform_WordCount {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);


        // TODO groupByKey方法作用是将KV类型的数据直接按照K对V进行分组
        //      (b,[2, 4])
        //      (a,[1, 3])
        JavaPairRDD<String, Integer> rdd = javaSparkContext.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("a", 3),
                new Tuple2<>("b", 4)
        ));

        rdd.groupByKey().mapValues(
                iter -> {
                    int sum = 0;
                    for (Integer integer : iter) {
                        sum += integer;
                    }
                    return sum;
                }
        ).collect().forEach(System.out::println);


        javaSparkContext.close();
    }
}
