package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark08_Operate_Transform_KV {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // TODO KV类型一般表示2元组
        //      Spark RDD会整体数据的处理就称之为单值类型的数据处理
        //      Spark RDD会KV数据个体的处理就称之为KV类型的数据处理： K 和 V 不作为整体使用
        Tuple2<String, Integer> a = new Tuple2<>("a", 1); // a, 2
        Tuple2<String, Integer> b = new Tuple2<>("b", 2); // b, 4
        Tuple2<String, Integer> c = new Tuple2<>("c", 3); // c, 6

        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(a, b, c);

//        JavaRDD<Tuple2<String, Integer>> javaRDD = javaSparkContext.parallelize(tuple2s, 2);
//
//        javaRDD.map(
//                t -> new Tuple2<>(t._1, t._2 * 2)
//        )
//                .collect().forEach(System.out::println);

        // TODO 上面的代码不是对KV类型的数据进行操作，还是单值类型的数据操作，是将2元组当成一个整体来使用。

        JavaPairRDD<String, Integer> PairRDD = javaSparkContext.parallelizePairs(tuple2s);
        // TODO mapValues：将KV类型的数据的value进行操作，key保持不变
        PairRDD.mapValues(
                t -> t * 2
        )
                        .collect()
                        .forEach(System.out::println);


        javaSparkContext.close();
    }
}
