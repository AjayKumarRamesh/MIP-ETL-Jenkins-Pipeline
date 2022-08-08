
package jobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}

import java.sql.DriverManager

object RubyToMIP extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")

  @throws(classOf[Exception])
  def runJobSequence(xtrSql: String, tgtSql: String, sourceDB: String, targetDB: String) = {

    log.info("runJobSequence started")
    log.info("THIS IS A TEST RUN FOR THIS BRANCH.")

    // create connection to CMDP database
    val srcConn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, "CMDP_PROD")
    srcConn.setProperty("sslConnection", "true")
    val tgtDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession,
      srcConn,
      xtrSql,
      Constants.PartitionTypeByColumn,
      null,
      "PARTCOL").drop("PARTCOL")

    tgtDF.show()

    // create connection to MIP database
    val tgtConn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, targetDB)
    tgtConn.setProperty("sslConnection", "true")

    // run SQL to merge dataframe into target table
    DataUtilities.runPreparedStatement(
      tgtConn,
      tgtDF,
      tgtSql,
      tgtDF.columns,
      null,
      null,
      null)

    val sourceRowCount = tgtDF.count().toInt
    val targetRowCount = sourceRowCount
    val mipDbEndpoint: String = tgtConn.getProperty(PropertyNames.EndPoint)

    recordRowCounts(sourceRowCount, targetRowCount, mipDbEndpoint)

  }


  def main(args: Array[String]): Unit = {
    var isJobFailed: Boolean = false

    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobStarted,
        s"$jobClassName Started", null, null)


      var sourceDB: String = null
      val sourceDBIndex = args.indexOf("--sourceDB")

      if (sourceDBIndex >= 0) {
        log.info(s"Arg 'sourceDB' found at index: $sourceDBIndex")
        if (args.length > (sourceDBIndex + 1)) {
          sourceDB = args(sourceDBIndex + 1)
          log.info(s"Value for arg 'sourceDB' value: $sourceDB")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'sourceDB' could not be found. Please pass the value."
          )
        }
      }

      var targetDB: String = null
      val targetDBIndex = args.indexOf("--targetDB")

      if (targetDBIndex >= 0) {
        log.info(s"Arg 'targetDB' found at index: $targetDBIndex")
        if (args.length > (targetDBIndex + 1)) {
          targetDB = args(targetDBIndex + 1)
          log.info(s"Value for arg 'targetDB' value: $targetDB")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'targetDB' could not be found. Please pass the value."
          )
        }
      }


      var xtrSql: String = null
      val xtrSqlIndex = args.indexOf("--xtrSql")

      if (xtrSqlIndex >= 0) {
        log.info(s"Arg 'xtrSql' found at index: $xtrSqlIndex")
        if (args.length > (xtrSqlIndex + 1)) {
          xtrSql = args(xtrSqlIndex + 1)
          log.info(s"Value for arg 'xtrSql' value: $xtrSql")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'xtrSql' could not be found. Please pass the value."
          )
        }
      }

      var tgtSql: String = null
      val tgtSqlIndex = args.indexOf("--tgtSql")

      if (tgtSqlIndex >= 0) {
        log.info(s"Arg 'tgtSql' found at index: $tgtSqlIndex")
        if (args.length > (tgtSqlIndex + 1)) {
          tgtSql = args(tgtSqlIndex + 1)
          log.info(s"Value for arg 'tgtSql' value: $tgtSql")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'tgtSql' could not be found. Please pass the value."
          )
        }
      }

      runJobSequence(xtrSql, tgtSql, sourceDB, targetDB)

      /*
        Note: You have access to the following instances:
        - CommonDBConProperties (connection properties to connect to App Config DB)
        - commonJobSeqCode (job sequence code)
        - sparkSession (spark session instance)

        At this point, you should have access to Common functions
        e.g: this.getDataSourceDetails(sparkSession, dataSourceCode = "ISAP")

        If you need to extract and save/write data, then use the Object: DataUtilities
        This object is a facade to all functions available.
        e.g: DataUtilities.readDataWithColumnPartitioning(...)
      */


      // Log job status POST - DB
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobSucceeded,
        s"Completed Job => $jobClassName.", null, null)


      log.info(s"Completed Job => $jobClassName.")

    } catch {
      case e: Throwable => {
        isJobFailed = true
        e.printStackTrace
        log.error(e.getMessage + " - " + e.getCause)
        DataUtilities.recordJobHistory(AppProperties.SparkSession,
          AppProperties.CommonJobSeqCode,
          0,
          Constants.JobFailed,
          e.getMessage + " - " + e.getCause, null, null)
      }
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")

      if (isJobFailed) {
        System.exit(1)
      }
    }
  }

  @throws(classOf[Exception])
  def recordRowCounts(sourceRowCount: Int, targetRowCount: Int, mipDbEndpoint: String) = {

    // Get JOB_SK value. This will be the row ID for the current record in the ETL_JOB_HIST table.
    var jobSK: String = null
    val sqlJobSK = "SELECT MAX(JOB_SK) FROM MAP_ETL.ETL_JOB_HIST WHERE JOB_SEQUENCE = '" +
      AppProperties.CommonJobSeqCode + "'"

    try {
      val mipDBConn = DriverManager.getConnection(mipDbEndpoint, AppProperties.CommonDBConProperties)
      val stmt = mipDBConn.createStatement
      val jobSKResultSet = stmt.executeQuery(sqlJobSK)
      if (jobSKResultSet.next()) {
        jobSK = jobSKResultSet.getString(1)
      }

      // Now update this record with our source and target row counts.
      val sqlUpdateRowCounts = "UPDATE MAP_ETL.ETL_JOB_HIST SET " +
        "SOURCE_ROW_COUNT = '" + sourceRowCount + "', " +
        "TARGET_ROW_COUNT = '" + targetRowCount + "' " +
        "WHERE JOB_SK = '" + jobSK + "'"
      stmt.execute(sqlUpdateRowCounts)
    }
    catch {
      case e: Throwable => {
        log.info("Failed to write row counts to Job History table.")
        e.printStackTrace()
      }
    }
  }
}