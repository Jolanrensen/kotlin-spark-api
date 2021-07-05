package org.jetbrains.kotlinx.spark.api


import org.apache.spark.api.java.function.*
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.jetbrains.kotinx.spark.extensions.KSparkExtensions
import scala.*
import scala.Function2
import scala.collection.Seq
import scala.collection.TraversableOnce
import scala.collection.immutable.List
import scala.reflect.api.TypeTags
import kotlin.reflect.KProperty1

class KDataset<T>(
    val dataset: Dataset<T>,
) : Dataset<T>(dataset.queryExecution(), dataset.encoder()) {

    /**
     * Selects column based on the column name and returns it as a [Column].
     *
     * @note The column name can also reference to a nested column like `a.b`.
     */
    operator fun invoke(colName: String): Column = col(colName)

    /**
     * Helper function to quickly get a [TypedColumn] (or [Column]) from a dataset in a refactor-safe manner.
     * ```kotlin
     *    val dataset: Dataset<YourClass> = ...
     *    val columnA: TypedColumn<YourClass, TypeOfA> = dataset(YourClass::a)
     * ```
     * @see col
     */
    inline operator fun <reified U> invoke(column: KProperty1<T, U>): TypedColumn<T, U> = col(column)

    /**
     * Helper function to quickly get a [TypedColumn] (or [Column]) from a dataset in a refactor-safe manner.
     * ```kotlin
     *    val dataset: Dataset<YourClass> = ...
     *    val columnA: TypedColumn<YourClass, TypeOfA> = dataset.col(YourClass::a)
     * ```
     * @see invoke
     */

    @Suppress("UNCHECKED_CAST")
    inline fun <reified U> col(column: KProperty1<T, U>): TypedColumn<T, U> =
        col(column.name).`as`<U>() as TypedColumn<T, U>

    override fun toDF(): KDataset<Row> {
        return super.toDF().toKotlin()
    }

    override fun <U : Any?> `as`(encoder: Encoder<U>): KDataset<U> {
        return super.`as`(encoder).toKotlin()
    }

    inline fun <reified U : Any?> `as`(): KDataset<U> {
        return `as`(encoder<U>())
    }

    fun <U : Any?> to(encoder: Encoder<U>): KDataset<U> {
        return `as`(encoder)
    }

    inline fun <reified U : Any?> to(): KDataset<U> {
        return `as`(encoder<U>())
    }

    fun <U : Any?> downcast(encoder: Encoder<U>): KDataset<U> {
        return `as`(encoder)
    }

    inline fun <reified U : Any?> downcast(): KDataset<U> {
        return `as`(encoder<U>())
    }

    override fun toDF(vararg colNames: String): KDataset<Row> {
        return super.toDF(*colNames).toKotlin()
    }

    override fun localCheckpoint(): KDataset<T> {
        return super.localCheckpoint().toKotlin()
    }

    override fun localCheckpoint(eager: Boolean): KDataset<T> {
        return super.localCheckpoint(eager).toKotlin()
    }

    override fun withWatermark(eventTime: String, delayThreshold: String): KDataset<T> {
        return super.withWatermark(eventTime, delayThreshold).toKotlin()
    }

    override fun join(right: Dataset<*>): KDataset<Row> {
        return super.join(right).toKotlin()
    }

    override fun join(right: Dataset<*>, usingColumn: String): KDataset<Row> {
        return super.join(right, usingColumn).toKotlin()
    }

    override fun join(right: Dataset<*>, usingColumns: Seq<String>): KDataset<Row> {
        return super.join(right, usingColumns).toKotlin()
    }

    override fun join(right: Dataset<*>, usingColumns: Seq<String>, joinType: String): KDataset<Row> {
        return super.join(right, usingColumns, joinType).toKotlin()
    }

    override fun join(right: Dataset<*>, joinExprs: Column): KDataset<Row> {
        return super.join(right, joinExprs).toKotlin()
    }

    override fun join(right: Dataset<*>, joinExprs: Column, joinType: String): KDataset<Row> {
        return super.join(right, joinExprs, joinType).toKotlin()
    }

    override fun crossJoin(right: Dataset<*>): KDataset<Row> {
        return super.crossJoin(right).toKotlin()
    }

    override fun <U : Any?> joinWith(other: Dataset<U>, condition: Column, joinType: String): KDataset<Tuple2<T, U>> {
        return super.joinWith(other, condition, joinType).toKotlin()
    }

    override fun <U : Any?> joinWith(other: Dataset<U>, condition: Column): KDataset<Tuple2<T, U>> {
        return super.joinWith(other, condition).toKotlin()
    }


    /**
     * Alias for [Dataset.joinWith] which passes "left" argument
     * and respects the fact that in result of left join right relation is nullable
     *
     * @receiver left dataset
     * @param right right dataset
     * @param col join condition
     *
     * @return dataset of pairs where right element is forced nullable
     */
    inline fun <reified R : Any?> leftJoin(right: Dataset<R>, col: Column): KDataset<Pair<T, R?>> {
        return joinWith(right, col, "left").map { it._1 to it._2 }
    }

    /**
     * Alias for [Dataset.joinWith] which passes "right" argument
     * and respects the fact that in result of right join left relation is nullable
     *
     * @receiver left dataset
     * @param right right dataset
     * @param col join condition
     *
     * @return dataset of [Pair] where left element is forced nullable
     */
    inline fun <reified R> rightJoin(right: Dataset<R>, col: Column): Dataset<Pair<T?, R>> {
        return joinWith(right, col, "right").map { it._1 to it._2 }
    }

    /**
     * Alias for [Dataset.joinWith] which passes "inner" argument
     *
     * @receiver left dataset
     * @param right right dataset
     * @param col join condition
     *
     * @return resulting dataset of [Pair]
     */
    inline fun <reified R> innerJoin(right: Dataset<R>, col: Column): Dataset<Pair<T, R>> {
        return joinWith(right, col, "inner").map { it._1 to it._2 }
    }

    /**
     * Alias for [Dataset.joinWith] which passes "full" argument
     * and respects the fact that in result of join any element of resulting tuple is nullable
     *
     * @receiver left dataset
     * @param right right dataset
     * @param col join condition
     *
     * @return dataset of [Pair] where both elements are forced nullable
     */
    inline fun <reified R : Any?> fullJoin(
        right: Dataset<R>,
        col: Column,
    ): Dataset<Pair<T?, R?>> {
        return joinWith(right, col, "full").map { it._1 to it._2 }
    }

    override fun sortWithinPartitions(sortCol: String, vararg sortCols: String): KDataset<T> {
        return super.sortWithinPartitions(sortCol, *sortCols).toKotlin()
    }

    override fun sortWithinPartitions(vararg sortExprs: Column): KDataset<T> {
        return super.sortWithinPartitions(*sortExprs).toKotlin()
    }

    override fun sort(sortCol: String, vararg sortCols: String): KDataset<T> {
        return super.sort(sortCol, *sortCols).toKotlin()
    }

    override fun sort(vararg sortExprs: Column): KDataset<T> {
        return super.sort(*sortExprs).toKotlin()
    }

    /**
     * Allows to sort data class dataset on one or more of the properties of the data class.
     * ```kotlin
     * val sorted: Dataset<YourClass> = unsorted.sort(YourClass::a)
     * val sorted2: Dataset<YourClass> = unsorted.sort(YourClass::a, YourClass::b)
     * ```
     */
    fun sort(col: KProperty1<T, *>, vararg cols: KProperty1<T, *>): KDataset<T> =
        sort(col.name, *cols.map { it.name }.toTypedArray())

    /**
     * Alias for [Dataset.sort] which forces user to provide sorted columns from the source dataset
     *
     * @receiver source [Dataset]
     * @param columns producer of sort columns
     * @return sorted [Dataset]
     */
    fun sort(columns: (KDataset<T>) -> Array<Column>): KDataset<T> = sort(*columns(this))


    override fun orderBy(sortCol: String, vararg sortCols: String): KDataset<T> {
        return super.orderBy(sortCol, *sortCols).toKotlin()
    }

    override fun orderBy(vararg sortExprs: Column): KDataset<T> {
        return super.orderBy(*sortExprs).toKotlin()
    }

    override fun hint(name: String, vararg parameters: Any): KDataset<T> {
        return super.hint(name, *parameters).toKotlin()
    }

    override fun `as`(alias: String): KDataset<T> {
        return super.`as`(alias).toKotlin()
    }

    override fun `as`(alias: Symbol): KDataset<T> {
        return super.`as`(alias).toKotlin()
    }

    override fun alias(alias: String): KDataset<T> {
        return super.alias(alias).toKotlin()
    }

    override fun alias(alias: Symbol): KDataset<T> {
        return super.alias(alias).toKotlin()
    }

    override fun select(vararg cols: Column): Dataset<Row> {
        return super.select(*cols).toKotlin()
    }

    override fun select(col: String, vararg cols: String): KDataset<Row> {
        return super.select(col, *cols).toKotlin()
    }

    override fun selectExpr(vararg exprs: String): KDataset<Row> {
        return super.selectExpr(*exprs).toKotlin()
    }

    override fun <U1 : Any?> select(c1: TypedColumn<T, U1>): KDataset<U1> {
        return super.select(c1).toKotlin()
    }

    override fun selectUntyped(columns: Seq<TypedColumn<*, *>>): KDataset<*> {
        return super.selectUntyped(columns).toKotlin()
    }


    /**
     * Returns a new Dataset by computing the given [Column] expressions for each element.
     */
    fun <U1 : Any?, U2 : Any?> selectTyped(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
    ): KDataset<Pair<U1, U2>>  {
        return super.select(c1, c2).toKotlin().map { Pair(it._1(), it._2()) }
    }

    @Deprecated("use selectTyped to get Kotlin Pairs", ReplaceWith("selectTyped(c1, c2)"))
    override fun <U1 : Any?, U2 : Any?> select(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
    ): KDataset<Tuple2<U1, U2>> {
        return super.select(c1, c2).toKotlin()
    }

    /**
     * Returns a new Dataset by computing the given [Column] expressions for each element.
     */
    fun <U1 : Any?, U2 : Any?, U3 : Any?> selectTyped(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
        c3: TypedColumn<T, U3>,
    ): KDataset<Triple<U1, U2, U3>> {
        return super.select(c1, c2, c3).toKotlin().map { Triple(it._1(), it._2(), it._3()) }
    }

    @Deprecated("use selectTyped to get Kotlin Triples", ReplaceWith("selectTyped(c1, c2, c3)"))
    override fun <U1 : Any?, U2 : Any?, U3 : Any?> select(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
        c3: TypedColumn<T, U3>,
    ): KDataset<Tuple3<U1, U2, U3>> {
        return super.select(c1, c2, c3).toKotlin()
    }

    fun <U1 : Any?, U2 : Any?, U3 : Any?, U4 : Any?> selectTyped(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
        c3: TypedColumn<T, U3>,
        c4: TypedColumn<T, U4>,
    ): KDataset<Arity4<U1, U2, U3, U4>> {
        return super.select(c1, c2, c3, c4).toKotlin().map { Arity4(it._1(), it._2(), it._3(), it._4()) }
    }

    @Deprecated("use selectTyped to get Arity4s", ReplaceWith("selectTyped(c1, c2, c3, c4)"))
    override fun <U1 : Any?, U2 : Any?, U3 : Any?, U4 : Any?> select(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
        c3: TypedColumn<T, U3>,
        c4: TypedColumn<T, U4>,
    ): KDataset<Tuple4<U1, U2, U3, U4>> {
        return super.select(c1, c2, c3, c4).toKotlin()
    }

    fun <U1 : Any?, U2 : Any?, U3 : Any?, U4 : Any?, U5 : Any?> selectTyped(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
        c3: TypedColumn<T, U3>,
        c4: TypedColumn<T, U4>,
        c5: TypedColumn<T, U5>,
    ): KDataset<Arity5<U1, U2, U3, U4, U5>> {
        return super.select(c1, c2, c3, c4, c5).toKotlin().map { Arity5(it._1(), it._2(), it._3(), it._4(), it._5()) }
    }

    @Deprecated("use selectTyped to get Arity5s", ReplaceWith("selectTyped(c1, c2, c3, c4)"))
    override fun <U1 : Any?, U2 : Any?, U3 : Any?, U4 : Any?, U5 : Any?> select(
        c1: TypedColumn<T, U1>,
        c2: TypedColumn<T, U2>,
        c3: TypedColumn<T, U3>,
        c4: TypedColumn<T, U4>,
        c5: TypedColumn<T, U5>,
    ): KDataset<Tuple5<U1, U2, U3, U4, U5>> {
        return super.select(c1, c2, c3, c4, c5).toKotlin()
    }

    override fun filter(condition: Column): KDataset<T> {
        return super.filter(condition).toKotlin()
    }

    override fun filter(conditionExpr: String): KDataset<T> {
        return super.filter(conditionExpr).toKotlin()
    }

    override fun where(condition: Column): KDataset<T> {
        return super.where(condition).toKotlin()
    }

    override fun where(conditionExpr: String): KDataset<T> {
        return super.where(conditionExpr).toKotlin()
    }

    fun reduce(func: (T, T) -> T): T {
        return super.reduce(ReduceFunction(func))
    }

    inline fun <reified K : Any?> groupByKey(
        noinline func: (T) -> K,
    ): KeyValueGroupedDataset<K, T> {
        return groupByKey(MapFunction(func), encoder<K>())
    }

    override fun <K : Any?> groupByKey(func: MapFunction<T, K>, encoder: Encoder<K>): KeyValueGroupedDataset<K, T> {
        return super.groupByKey(func, encoder)
    }

    override fun agg(aggExpr: Tuple2<String, String>, aggExprs: Seq<Tuple2<String, String>>): KDataset<Row> {
        return super.agg(aggExpr, aggExprs).toKotlin()
    }

    override fun agg(exprs: Map<String, String>): KDataset<Row> {
        return super.agg(exprs).toKotlin()
    }

    override fun agg(expr: Column, vararg exprs: Column): KDataset<Row> {
        return super.agg(expr, *exprs).toKotlin()
    }

    override fun observe(name: String, expr: Column, exprs: Seq<Column>): KDataset<T> {
        return super.observe(name, expr, exprs).toKotlin()
    }

    override fun limit(n: Int): KDataset<T> {
        return super.limit(n).toKotlin()
    }

    fun <R : Dataset<T>> union(other: R): KDataset<T> {
        return super.union(other).toKotlin()
    }

    fun <R : Dataset<T>> unionAll(other: R): KDataset<T> {
        return super.unionAll(other).toKotlin()
    }

    fun <R : Dataset<T>> unionByName(other: R): KDataset<T> {
        return super.unionByName(other).toKotlin()
    }

    fun <R : Dataset<T>> intersect(other: R): KDataset<T> {
        return super.intersect(other).toKotlin()
    }

    fun <R : Dataset<T>> intersectAll(other: R): KDataset<T> {
        return super.intersectAll(other).toKotlin()
    }

    fun <R : Dataset<T>> except(other: R): KDataset<T> {
        return super.except(other).toKotlin()
    }

    fun <R : Dataset<T>> exceptAll(other: R): KDataset<T> {
        return super.exceptAll(other).toKotlin()
    }

    override fun sample(fraction: Double, seed: Long): KDataset<T> {
        return super.sample(fraction, seed).toKotlin()
    }

    override fun sample(fraction: Double): KDataset<T> {
        return super.sample(fraction).toKotlin()
    }

    override fun sample(withReplacement: Boolean, fraction: Double, seed: Long): KDataset<T> {
        return super.sample(withReplacement, fraction, seed).toKotlin()
    }

    override fun sample(withReplacement: Boolean, fraction: Double): KDataset<T> {
        return super.sample(withReplacement, fraction).toKotlin()
    }

    override fun randomSplit(weights: DoubleArray, seed: Long): Array<KDataset<T>> {
        return super.randomSplit(weights, seed).run {
            Array(size) { this[it].toKotlin() }
        }
    }

    override fun randomSplitAsList(weights: DoubleArray, seed: Long): MutableList<KDataset<T>> {
        return super.randomSplitAsList(weights, seed).run {
            MutableList(size) { this[it].toKotlin() }
        }
    }

    override fun randomSplit(weights: DoubleArray): Array<KDataset<T>> {
        return super.randomSplit(weights).run {
            Array(size) { this[it].toKotlin() }
        }
    }

    /**
     * This function creates block, where one can call any further computations on already cached dataset
     * Data will be unpersisted automatically at the end of computation
     *
     * it may be useful in many situations, for example, when one needs to write data to several targets
     * ```kotlin
     * ds.withCached {
     *   write()
     *      .also { it.orc("First destination") }
     *      .also { it.avro("Second destination") }
     * }
     * ```
     *
     * @param blockingUnpersist if execution should be blocked until everything persisted will be deleted
     * @param executeOnCached Block which should be executed on cached dataset.
     * @return result of block execution for further usage. It may be anything including source or new dataset
     */
    fun <R> withCached(
        blockingUnpersist: Boolean = false,
        executeOnCached: KDataset<T>.() -> R,
    ): R {
        val cached = this.cache()
        return cached.executeOnCached().also { cached.unpersist(blockingUnpersist) }
    }

    override fun <A : Product> explode(
        input: Seq<Column>,
        f: Function1<Row, TraversableOnce<A>>,
        `evidence$4`: TypeTags.TypeTag<A>,
    ): KDataset<Row> {
        return super.explode(input, f, `evidence$4`).toKotlin()
    }

    override fun <A : Any?, B : Any?> explode(
        inputColumn: String,
        outputColumn: String,
        f: Function1<A, TraversableOnce<B>>,
        `evidence$5`: TypeTags.TypeTag<B>,
    ): KDataset<Row> {
        return super.explode(inputColumn, outputColumn, f, `evidence$5`).toKotlin()
    }

    override fun withColumn(colName: String, col: Column): KDataset<Row> {
        return super.withColumn(colName, col).toKotlin()
    }

    override fun withColumns(colNames: Seq<String>, cols: Seq<Column>): KDataset<Row> {
        return super.withColumns(colNames, cols).toKotlin()
    }

    override fun withColumns(colNames: Seq<String>, cols: Seq<Column>, metadata: Seq<Metadata>): KDataset<Row> {
        return super.withColumns(colNames, cols, metadata).toKotlin()
    }

    override fun withColumn(colName: String, col: Column, metadata: Metadata): KDataset<Row> {
        return super.withColumn(colName, col, metadata).toKotlin()
    }

    override fun withColumnRenamed(existingName: String, newName: String): KDataset<Row> {
        return super.withColumnRenamed(existingName, newName).toKotlin()
    }

    override fun drop(colName: String): KDataset<Row> {
        return super.drop(colName).toKotlin()
    }

    override fun drop(vararg colNames: String): KDataset<Row> {
        return super.drop(*colNames).toKotlin()
    }

    override fun drop(col: Column): KDataset<Row> {
        return super.drop(col).toKotlin()
    }

    override fun dropDuplicates(): KDataset<T> {
        return super.dropDuplicates().toKotlin()
    }

    override fun dropDuplicates(colNames: Seq<String>): KDataset<T> {
        return super.dropDuplicates(colNames).toKotlin()
    }

    override fun dropDuplicates(colNames: Array<out String>): KDataset<T> {
        return super.dropDuplicates(colNames).toKotlin()
    }

    override fun dropDuplicates(col1: String, vararg cols: String): KDataset<T> {
        return super.dropDuplicates(col1, *cols).toKotlin()
    }

    override fun describe(vararg cols: String): KDataset<Row> {
        return super.describe(*cols).toKotlin()
    }

    override fun summary(vararg statistics: String): KDataset<Row> {
        return super.summary(*statistics).toKotlin()
    }

    fun <U : Any?> transform(t: (KDataset<T>) -> KDataset<U>): KDataset<U> {
        return super.transform { t(it.toKotlin()) }.toKotlin()
    }

    fun forEach(func: (T) -> Unit) = foreach(ForeachFunction(func))

    fun forEachPartition(func: (Iterator<T>) -> Unit) =
        foreachPartition(ForeachPartitionFunction(func))


    override fun filter(func: Function1<T, Any>): KDataset<T> {
        return super.filter(func).toKotlin()
    }

    override fun filter(func: FilterFunction<T>): KDataset<T> {
        return super.filter(func).toKotlin()
    }

    inline fun <reified U : Any?> map(noinline func: (T) -> U): KDataset<U> {
        return map(MapFunction(func), encoder<U>()).toKotlin()
    }

    inline fun <reified U : Any?> mapPartitions(
        noinline func: (Iterator<T>) -> Iterator<U>,
    ): KDataset<U> {
        return mapPartitions(MapPartitionsFunction(func), encoder<U>()).toKotlin()
    }

    override fun <U : Any?> mapPartitions(f: MapPartitionsFunction<T, U>, encoder: Encoder<U>): KDataset<U> {
        return super.mapPartitions(f, encoder).toKotlin()
    }

    override fun mapPartitionsInR(
        func: ByteArray,
        packageNames: ByteArray,
        broadcastVars: Array<out Broadcast<Any>>,
        schema: StructType,
    ): KDataset<Row> {
        return super.mapPartitionsInR(func, packageNames, broadcastVars, schema).toKotlin()
    }

    override fun mapInPandas(func: PythonUDF): KDataset<Row> {
        return super.mapInPandas(func).toKotlin()
    }


    inline fun <reified U : Any?> flatMap(noinline func: (T) -> Iterator<U>): KDataset<U> {
        return flatMap(FlatMapFunction(func), encoder<U>()).toKotlin()
    }

    override fun repartition(numPartitions: Int): KDataset<T> {
        return super.repartition(numPartitions).toKotlin()
    }

    override fun repartition(numPartitions: Int, vararg partitionExprs: Column): KDataset<T> {
        return super.repartition(numPartitions, *partitionExprs).toKotlin()
    }

    override fun repartition(vararg partitionExprs: Column): KDataset<T> {
        return super.repartition(*partitionExprs).toKotlin()
    }

    override fun repartitionByRange(numPartitions: Int, vararg partitionExprs: Column): KDataset<T> {
        return super.repartitionByRange(numPartitions, *partitionExprs).toKotlin()
    }

    override fun repartitionByRange(vararg partitionExprs: Column): KDataset<T> {
        return super.repartitionByRange(*partitionExprs).toKotlin()
    }

    override fun coalesce(numPartitions: Int): KDataset<T> {
        return super.coalesce(numPartitions).toKotlin()
    }

    override fun distinct(): KDataset<T> {
        return super.distinct().toKotlin()
    }

    override fun persist(): KDataset<T> {
        return super.persist().toKotlin()
    }

    override fun cache(): KDataset<T> {
        return super.cache().toKotlin()
    }

    override fun persist(newLevel: StorageLevel): KDataset<T> {
        return super.persist(newLevel).toKotlin()
    }

    override fun unpersist(blocking: Boolean): KDataset<T> {
        return super.unpersist(blocking).toKotlin()
    }

    override fun unpersist(): KDataset<T> {
        return super.unpersist().toKotlin()
    }

    /**
     * Alternative to [Dataset.show] which returns source dataset.
     * Useful for debug purposes when you need to view content of a dataset as an intermediate operation
     */
    fun showDS(numRows: Int = 20, truncate: Boolean = true) = apply { show(numRows, truncate) }

    /**
     * It's hard to call `Dataset.debugCodegen` from kotlin, so here is utility for that
     */
    fun debugCodegen() = also { KSparkExtensions.debugCodegen(it) }

    /**
     * It's hard to call `Dataset.debug` from kotlin, so here is utility for that
     */
    fun debug() = also { KSparkExtensions.debug(it) }
}

inline fun <T> Dataset<T>.toKotlin(): KDataset<T> = when (this) {
    is KDataset<T> -> this
    else -> KDataset(this)
}
