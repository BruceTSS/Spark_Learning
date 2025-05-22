package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark05_Operate_Transform_groupBy_2 {
    public static void main(String[] args) throws InterruptedException {
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
               nums,3
        );

        // TODO RDD的方法 ： groupBy，按照指定的规则对数据进行分组
        // 含有shuffle操作的方法都具有改变分区的能力，可以设定分区参数
        javaRDD.
                groupBy( num -> num % 2 == 0, 2)
                .collect().
                forEach(System.out::println);

        System.out.println("计算完毕");
        // 监控页面：http://localhost:4040
        Thread.sleep(1000000000);





        javaSparkContext.close();
    }
}
