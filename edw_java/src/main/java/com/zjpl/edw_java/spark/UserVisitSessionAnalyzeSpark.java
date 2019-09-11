package com.zjpl.edw_java.spark;

import com.zjpl.edw_java.constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        SparkConf sparkConf= new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.close();
    }
}
