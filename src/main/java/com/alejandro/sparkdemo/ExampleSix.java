package com.alejandro.sparkdemo;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class ExampleSix {

	public static void main(String[] args) {
		
		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather06").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();
		
		final String PATH = "/home/alejandro/eclipse-workspace/sparkdemo/src/main/resources/test.txt";
		Dataset<String> datosMeteorologicos = sqlContext.read().textFile(PATH);
		List<String> list = datosMeteorologicos.as(Encoders.STRING()).collectAsList();
		List<String> codes = new ArrayList<String>();
		for(String s : list) {
			if(s.startsWith("333")) {
				codes.add(s);
			}
		}
		codes.forEach(s -> System.out.println(s));
		spark.close();
	}

}
