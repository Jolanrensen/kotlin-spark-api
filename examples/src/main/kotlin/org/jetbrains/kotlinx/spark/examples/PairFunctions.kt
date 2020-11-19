package org.jetbrains.kotlinx.spark.examples

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.jetbrains.kotlinx.spark.api.*
import scala.Tuple2


fun main() = withSpark {
//    val pairs = JavaSparkContext(spark.sparkContext).parallelize()
}


inline fun <reified K, reified V> RDD<Tuple2<K, V>>.toJavaPairRDD(): JavaPairRDD<K, V> =
        JavaPairRDD.fromRDD(this, encoder<K>().clsTag(), encoder<V>().clsTag())
