# Databricks notebook source

# MAGIC %scala
# MAGIC import scala.util.matching.Regex
# MAGIC import org.apache.spark.sql.Dataset
# MAGIC import com.amazon.deequ.{VerificationSuite, VerificationResult}
# MAGIC import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
# MAGIC import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
# MAGIC import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
# MAGIC import com.amazon.deequ.analyzers._
# MAGIC import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
# MAGIC import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
# MAGIC import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Compliance, Distinctness, InMemoryStateProvider, Size}
# MAGIC import com.amazon.deequ.constraints.ConstrainableDataTypes

# COMMAND ----------

# MAGIC %scala
# MAGIC val suggestionResult = ConstraintSuggestionRunner()
# MAGIC   .onData(spark.sql("SELECT * FROM bronze_lake_tenant_2_DB.b_trades"))
# MAGIC   .addConstraintRules(Rules.DEFAULT)
# MAGIC   .run()
# MAGIC 
# MAGIC suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
# MAGIC   suggestions.foreach { suggestion =>
# MAGIC     println(s"Constraint suggestion for '$column':\t${suggestion.description}\n" +
# MAGIC       s"The corresponding scala code is ${suggestion.codeForConstraint}\n")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC val analysisResult: AnalyzerContext = { AnalysisRunner
# MAGIC   // data to run the analysis on
# MAGIC   .onData(spark.sql("select * from bronze_lake_tenant_2_DB.b_trades"))
# MAGIC   // define analyzers that compute metrics
# MAGIC   .addAnalyzer(Size())
# MAGIC   .addAnalyzer(Completeness("trade_id"))
# MAGIC   .addAnalyzer(Completeness("Security_id"))
# MAGIC   .addAnalyzer(Completeness("client_id"))
# MAGIC   .addAnalyzer(Distinctness("trade_id"))
# MAGIC   .addAnalyzer(Distinctness("client_id"))
# MAGIC   .addAnalyzer(ApproxCountDistinct("client_id"))
# MAGIC   .addAnalyzer(DataType("Amount"))
# MAGIC   .addAnalyzer(Uniqueness("client_id"))
# MAGIC   .addAnalyzer(UniqueValueRatio("client_id"))
# MAGIC   .addAnalyzer(Mean("amount"))
# MAGIC   .addAnalyzer(Maximum("amount"))
# MAGIC   .addAnalyzer(Minimum("amount"))
# MAGIC   .addAnalyzer(StandardDeviation("amount"))
# MAGIC   .addAnalyzer(Correlation("Shares_qty", "amount"))
# MAGIC   .addAnalyzer(Compliance("securities quntitiy 200+", "Shares_qty >= 200.0"))
# MAGIC   // compute metrics
# MAGIC   .run()
# MAGIC }
# MAGIC // retrieve successfully computed metrics as a Spark data frame
# MAGIC val metrics = successMetricsAsDataFrame(spark, analysisResult)
# MAGIC display (metrics)
# MAGIC   
# MAGIC // write the current results into the metrics table
# MAGIC metrics.write.format("delta").mode("Overwrite").saveAsTable("data_quality_tenant_2_DB.deequ_metrics")

# COMMAND ----------

# MAGIC %scala
# MAGIC val verificationResult: VerificationResult = { VerificationSuite()
# MAGIC   .onData(spark.sql("select * from bronze_lake_tenant_2_DB.b_trades"))
# MAGIC   .addCheck(
# MAGIC     Check(CheckLevel.Error, "Review Check") 
# MAGIC       .hasMin("shares_qty", _ >= 10) // min is 10
# MAGIC       .hasMax("shares_qty", _ <= 300) // max is 300
# MAGIC       .hasCompleteness("shares_qty", _ >= 0.95) // should never be NULL
# MAGIC       .isUnique("trade_id") // should not contain duplicates
# MAGIC       // contains only the listed values
# MAGIC       .isContainedIn("client_id", Array("0XZR","6VYV","ABCD","B0G3","CGF8","E3JC","H2R6","H5K7","HPPG","LUZW","PSB4","SSKB","TCM3","UZJV","VLV1","WTGZ","ZJAD"))
# MAGIC       .isNonNegative("shares_qty")) // should not contain negative values
# MAGIC   .run()
# MAGIC }
# MAGIC 
# MAGIC // convert check results to a Spark data frame
# MAGIC val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
# MAGIC display(resultDataFrame)
