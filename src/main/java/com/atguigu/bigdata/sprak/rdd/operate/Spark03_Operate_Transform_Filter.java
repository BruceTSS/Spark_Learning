package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark03_Operate_Transform_Filter {
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

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(
               nums,2
        );

        // TODO RDD的转换方法：filter(过滤)
        //      RDD可以根据制定的过滤规则对数据源中的数据进行筛选过滤
        //      如果满足规则（返回结果为true），那么数据保留，如果不满足规则（返回结果为false），那么数据就会丢弃

//        JavaRDD<Integer> filterRDD = javaRDD.filter(new Function<Integer, Boolean>() {
//            @Override
//            public Boolean call(Integer num) throws Exception {
//                return true;
//            }
//        });

        // Map => A(String) ->  B(Integer, User, List)
        // Filter => A ->
        // Filter方法在执行过程中可能会出现数据倾斜的情况，需要慎重考虑
        JavaRDD<Integer> filterRDD = javaRDD.filter(
                num -> num  % 2 == 1
        );

        filterRDD.collect().forEach(System.out::println);


        javaSparkContext.close();
    }
}
