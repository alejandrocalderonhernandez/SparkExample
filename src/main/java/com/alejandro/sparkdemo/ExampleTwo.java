package com.alejandro.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.alejandro.sparkdemo.bean.DatosMeteorologicos;


public class ExampleTwo {
	
	public static void main(final String[] parametros) {

		final SparkConf sparkConf = new SparkConf().setAppName("SparkWeather02").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		Dataset<Row> datosMeteorologicos = obtenerDatos(sqlContext);
		final Dataset<DatosMeteorologicos> as = datosMeteorologicos.as(Encoders.bean(DatosMeteorologicos.class));

		as.createOrReplaceTempView("datos");
		datosMeteorologicos = sqlContext.sql("select Time, Indoor_Temperature from datos where Indoor_Temperature < 10");

		salvarDatos(datosMeteorologicos);
		spark.close();
	}

	private static void salvarDatos(final Dataset<Row> datosMeteorologicos) {
		datosMeteorologicos.write().json("src/main/resources/json");
	}

	private static Dataset<Row> obtenerDatos(final SparkSession sqlContext) {
		final String PATH = "/home/alejandro/eclipse-workspace/sparkdemo/src/main/resources/EasyWeather.txt";
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(PATH);
		return datosMeteorologicos;
	}

}

