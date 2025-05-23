package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class Spark13_Operate_Transform_sortByKey_1 {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        User user1 = new User();
        user1.age = 30;
        user1.amount = 2000;

        User user2 = new User();
        user2.age = 40;
        user2.amount = 3000;

        User user3 = new User();
        user3.age = 30;
        user3.amount = 3000;

        User user4 = new User();
        user4.age = 40;
        user4.amount = 2500;

        JavaPairRDD<User, Integer> rdd = javaSparkContext.parallelizePairs(Arrays.asList(
                new Tuple2<>(user1, 1),
                new Tuple2<>(user2, 2),
                new Tuple2<>(user3, 3),
                new Tuple2<>(user4, 4)
        ));

        // TODO sortByKey方法
        //      groupByKey  : 按照 K 对 V 进行分组
        //      reduceByKey : 按照 K 对 V 进行两两聚合
        //      sortByKey   : 按照 K 排序

        // TODO ClassCastException: com.atguigu.bigdata.sprak.rdd.operate.User cannot be cast to java.lang.Comparable

        // sortByKey方法要求数据中的K必须可以进行比较，实现Comparable接口
        rdd.sortByKey().collect().forEach(System.out::println);



        javaSparkContext.close();
    }
}

class User implements Serializable,  Comparable<User> {
    int age = 0;
    int amount = 0;

    @Override
    public String toString() {
        return "User{" +
                "age=" + age +
                ",amount=" + amount +
                "}";
    }

    @Override
    // 方法返回值为整型数据，表示数据比较结果（状态）
    // 如果为大于0的整数，那么表示当前对象比其他的对象大
    // 如果为小于0的整数，那么表示当前对象比其他的对象小
    // 如果等于0，那么表示当前对象与其他的对象一样大
    public int compareTo(User other) {
        if (this.age < other.age){
            return -1;
        }else if (this.age > other.age){
            return 1;
        }else {
            if (this.amount < other.amount) {
                return 1;
            }else if (this.amount > other.amount){
                return -1;
            }else {
                return 0;
            }
        }
    }
}
