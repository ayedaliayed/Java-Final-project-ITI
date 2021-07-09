/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.my_projet_final;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import scala.Tuple2;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import org.knowm.xchart.style.Styler;

/**
 *
 * @author Ayed Ali
 */
public class final_project {
    public static void main(String[] args) throws IOException {
        //System.out.println("SSSSSSSSSSSs");
        Logger.getLogger ("org").setLevel (Level.ERROR);
        final SparkSession sparkSession = SparkSession.builder().appName("C:\\Users\\Ayed Ali\\Documents\\NetBeansProjects\\my_projet_final\\Wuzzuf_Jobs.csv")
                .master("local[4]").getOrCreate();
        final DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
        final Dataset<Row> jobsData = dataFrameReader.csv("C:\\Users\\Ayed Ali\\Documents\\NetBeansProjects\\my_projet_final\\Wuzzuf_Jobs.csv");
        jobsData.printSchema();
        jobsData.show(20);
        jobsData.describe().show();
        //3 cleaning data
        Dataset<Row> cleanedJobsData = jobsData.na().drop();
        cleanedJobsData.describe().show();
        cleanedJobsData = cleanedJobsData.dropDuplicates();
        System.out.println("data after cleaning");
        Logger.getLogger ("org").setLevel (Level.ERROR);
        cleanedJobsData.describe().show();
        //4
        cleanedJobsData.createOrReplaceTempView ("job_view");
        Dataset<Row> jobPerCompany = sparkSession.sql("SELECT Company, count(Title) as number "+
                                                                  "FROM job_view GROUP BY Company "+
                                                                  "ORDER BY number DESC ");
        jobPerCompany.describe().show();
        jobPerCompany.show();
        
        //5
       
        List<String> companyNames = jobPerCompany.select("Company").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> jobCount = jobPerCompany.select("number").limit(10).as(Encoders.LONG()).collectAsList();
         PieChart pieChart1 = new PieChartBuilder().width(1280).height(800).title("Jobs Per Company").build();
                for (int i=0; i<companyNames.size(); i++){
            pieChart1.addSeries(companyNames.get(i), jobCount.get(i));
        }
        BitmapEncoder.saveBitmap(pieChart1, "C:\\Users\\Ayed Ali\\Documents\\NetBeansProjects\\my_projet_final", BitmapEncoder.BitmapFormat.PNG);
        new SwingWrapper (pieChart1).displayChart ();
    

  //6
           Dataset<Row> popularTitles = sparkSession.sql("SELECT Title, count(Title) as freq "+
                                                             "FROM job_view GROUP BY Title "+
                                                             "ORDER BY freq DESC ");
        popularTitles.show();
        //7
        List<String> jobTitles = popularTitles.select("Title").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> jobCount2 = popularTitles.select("freq").limit(10).as(Encoders.LONG()).collectAsList();
        CategoryChart barChart = new CategoryChartBuilder().width(1280).height(800).title("Popular Job Titles").build();
        barChart.addSeries("Popular Job Titles", jobTitles, jobCount2);
        BitmapEncoder.saveBitmap(barChart, "C:\\Users\\Ayed Ali\\Documents\\NetBeansProjects\\my_projet_final", BitmapEncoder.BitmapFormat.PNG);
        new SwingWrapper (barChart).displayChart ();
        //8
           Dataset<Row> popularLocations = sparkSession.sql("SELECT Location, count(Location) as freq "+
                                                                "FROM JOBS_DATA GROUP BY Location "+
                                                                "ORDER BY freq DESC ");
        popularLocations.show();
        //9
        List<String> locations = popularLocations.select("Location").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> locCount = popularTitles.select("freq").limit(10).as(Encoders.LONG()).collectAsList();
       barChart = new CategoryChartBuilder().width(1280).height(800).title("Popular Job Locations").build();
        barChart.addSeries("Popular Job Locations", locations, locCount);
        //BitmapEncoder.saveBitmap(barChart, "C:\\Users\\Ayed Ali\\Documents\\NetBeansProjects\\my_projet_final", BitmapEncoder.BitmapFormat.PNG);
        new SwingWrapper (barChart).displayChart ();        





//10
        List<String> skills = cleanedJobsData.select("Skills").map(row -> row.getString(0), Encoders.STRING()).collectAsList();
        Dataset<Row> popularSkills = cleanedJobsData.select("Skills")
                .flatMap(row -> Arrays.asList(row.getString(0).split(",")).iterator(), Encoders.STRING())
                .filter(s -> !s.isEmpty())
                .map(word -> new Tuple2<>(word.toLowerCase(), 1L), Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
                .toDF("word", "count")
                .groupBy("word")
                .sum("count").orderBy(new Column("sum(count)").desc()).withColumnRenamed("sum(count)", "cnt");
        popularSkills.show();
         //System .out,println("end");
    }
}
