package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark02_Operate_Transform_Map_1 {
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

        // TODO RDD方法
        // 【1,2】  【3,4】
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(
               nums,2
        );

        // TODO map方法的作用就是将传入的A转换为B返回，但是没有限制A和B的关系。
        JavaRDD<Integer> mapRDD = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer in) throws Exception {
                return in * 2;
            }
        });

        mapRDD.collect().forEach(System.out::println);
        System.out.println("=====================================");
        System.out.println(nums);


        javaSparkContext.close();
    }
}
