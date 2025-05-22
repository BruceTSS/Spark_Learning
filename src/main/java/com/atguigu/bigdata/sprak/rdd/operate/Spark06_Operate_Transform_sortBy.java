package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark06_Operate_Transform_sortBy {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> nums = Arrays.asList(1, 33, 3, 2, 4, 11);

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(
               nums,2
        );

        // TODO sortBy方法 ： 按照指定的排序规则对数据进行排序
        //      sortBy方法可以传递三个参数
        //          第一个参数表示排序规则
        //              spark会为每一个数据增加一个标记，然后按照标记对数据进行排序
        //          第二个参数表示排序的方式： true表示升序，false表示降序
        //          第三个参数表示分区数量

        /*
            1, 33, 3, 2, 4, 11
            -------------------------
            标记：1, 33, 3, 2, 4, 11
            =>
            标记：1, 2, 3, 4, 11, 33
            -------------------------
            数据：1, 2, 3, 4, 11, 33
            --------------------------------------------------
                 1,   33,   3,   2,   4,   11
            -------------------------
            标记："1", "33", "3", "2", "4", "11"
            =>
            标记："1", "11", "2", "3", "33", "4"
            -------------------------
            数据：1, 11, 2, 3, 33, 4
         */
//        javaRDD.sortBy(num -> num, false, 2).collect().forEach(System.out::println);

        javaRDD.sortBy(num -> "" + num, true, 2).collect().forEach(System.out::println);






        javaSparkContext.close();
    }
}
