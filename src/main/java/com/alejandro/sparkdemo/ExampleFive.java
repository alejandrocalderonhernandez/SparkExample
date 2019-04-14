package com.alejandro.sparkdemo;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.alejandro.sparkdemo.bean.DatosMeteorologicos;

public class ExampleFive {

	public static void main(String[] args) {
		
		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather01").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		final String PATH = "/home/alejandro/eclipse-workspace/sparkdemo/src/main/resources/EasyWeather.txt";
		Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(PATH);
		
		datosMeteorologicos = datosMeteorologicos.orderBy("Indoor_Humidity");
		List<DatosMeteorologicos> list = datosMeteorologicos.as(Encoders.bean(DatosMeteorologicos.class)).collectAsList();
		//list.forEach((s) -> System.out.println(s.toString()));
		List<DatosMeteorologicos> ls = new ArrayList<DatosMeteorologicos>();
		
		for(DatosMeteorologicos dm : list) {
			if(dm.toString().startsWith("DatosMeteorologicos [No=2, Time=24-01-2014 22:14, Interval=5")) {
				ls.add(dm);
			}
		}
		
		ls.forEach((s) -> System.out.println(s.toString()));

		
		spark.close();
	}

}
