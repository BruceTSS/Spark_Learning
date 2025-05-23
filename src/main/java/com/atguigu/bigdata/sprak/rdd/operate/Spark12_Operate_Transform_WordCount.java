package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class Spark12_Operate_Transform_WordCount {
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


        // TODO 将分组聚合功能进行简化操作
        //      reduceByKey方法的作用： 将KV类型的数据直接按照 K 对 V 进行reduce聚合（将多个值聚合成一个值）操作
        //      [1, 3] => 4
        //      计算的基本思想： 两两计算
        //      (i1, i2) => i3
//        rdd.reduceByKey(
//                Integer::sum
//        ).collect().forEach(System.out::println);
//        rdd.reduceByKey(
//                new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer v1, Integer v2) throws Exception {
//                        return v1 + v2;
//                    }
//                }
//        ).collect().forEach(System.out::println);
        rdd.reduceByKey(
                (v1, v2) -> v1 + v2
        ).collect().forEach(System.out::println);


        javaSparkContext.close();
    }
}
