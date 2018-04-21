package com.fymen.test
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object HelloSpark {

    def main(args: Array[String]) {
        val inputFile =  "file:///d:\\fm\\Desktop\\fuckshit.txt"
        val conf = new SparkConf().setAppName("WordCount").setMaster("spark://master:7077")
        val sc = new SparkContext(conf)
                val textFile = sc.textFile(inputFile)
                val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
                wordCount.foreach(println)       
    }
 }