package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark05_Operate_Transform_groupBy {
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

        // TODO RDD的方法 ： groupBy，按照指定的规则对数据进行分组
        javaRDD.groupBy(new Function<Integer, Object>() {
            @Override
            public Object call(Integer integer) throws Exception {
                //  返回的值其实就是数据对应的组的名称，相同的组的名称的数据会放置在一个组中
                //  当前的逻辑就是给数据增加标记
                // 1 -> B
                // 2 -> B
                // 3 -> B
                // 4 -> B
                return 'b';  //  组的名称，此处需要实现分组逻辑
            }
        })
                // 逻辑执行完毕后，打印的结果就只有一行(b,[1, 2, 3, 4])，
                // 一行数据就表示一个组，组的名称就是b，组中的数据就是1，2，3，4
                .collect().forEach(System.out::println);





        javaSparkContext.close();
    }
}
