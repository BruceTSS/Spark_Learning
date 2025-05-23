package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark09_Operate_Transform_KV_groupBy {
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

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(nums);
        // TODO groupBy: 按照指定的规则对数据进行分组
        //              给每一个数据增加一个标记，相同的标记的数据会放置在一个组中，这个标记就是组名
        //      groupBy结果： 就是KV类型的数据
        //          (0, 【2, 4】) => (0, 6)
        //          (1, 【1, 3】) => (1, 4)
        /*
            1 => 1
            2 => 0
            3 => 1
            4 => 0
         */
        JavaPairRDD<Integer, Iterable<Integer>> groupRDD = javaRDD.groupBy(num -> num % 2);

        groupRDD.mapValues(
                iter -> {
                    int sum = 0;
                    for (Integer num : iter) {
                        sum += num;
                    }
                    return sum;
                }
        ).collect().forEach(System.out::println);

        // TODO word -> count

        javaSparkContext.close();
    }
}
