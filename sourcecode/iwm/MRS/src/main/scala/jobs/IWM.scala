
package jobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import java.sql.DriverManager


object IWM extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")

  @throws(classOf[Exception])
  def runJobSequence(xtrRegSql: String, xtrAnsSql: String, uplRegSql: String, uplAnsSql: String, sourceDB: String, targetDB: String, env: String) = {

    log.info("runJobSequence started")
    log.info("xtr_iwm_registration started")

    // Connection to MRS Staging database and pull Registrations data
    log.info("sourceDB = " + sourceDB)
    val srcConn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, sourceDB)
    srcConn.setProperty("sslConnection", "true")
    val reg = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession,
      srcConn,
      xtrRegSql,
      Constants.PartitionTypeByColumn,
      null,
      "PARTCOL").drop("PARTCOL")

    log.info("Show reg df")
    val sourceRowCount = reg.count().toInt
    reg.show()

    // Create new dataframe with renamed columns. We will join it with Answers data.
    val regToJoin = reg.withColumnRenamed("SOURCE", "REG_SOURCE")
      .withColumnRenamed("DATASOURCE", "REG_DATASOURCE")

    // Pull Answers data from MRS Database
    val ansDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession,
      srcConn,
      xtrAnsSql,
      Constants.PartitionTypeByColumn,
      null,
      "PARTCOL").drop("PARTCOL")

    // Join Registrations and Answers data
    val ans = ansDF.join(regToJoin, ansDF("TRANSACTIONID") === regToJoin("TRANSACTIONID"), "inner").select(
      ansDF("TRANSACTIONID"), ansDF("SOURCE"), ansDF("QUESTIONNUM"), ansDF("QUESTION"), ansDF("ANSWER"),
      ansDF("DATASOURCE"), ansDF("RELATED_QUESTION"), ansDF("RELATED_ANSWER"), regToJoin("EVENTID"), regToJoin("IORDNUM")
    )

    // Build Question and Answers column pairing for Trial ID, Source System and Katakana questions
    val qacolsDF = ans.select(ans("QUESTIONNUM"), ans("TRANSACTIONID"), ans("SOURCE"), ans("DATASOURCE"), ans("ANSWER"))
      .filter("QUESTIONNUM = 'Q_TrialID' || QUESTIONNUM = 'Q_SOURCE_SYSTEM' || QUESTIONNUM = 'Q_KATAKANA'")

    qacolsDF.select(qacolsDF("QUESTIONNUM"), qacolsDF("TRANSACTIONID"), qacolsDF("SOURCE"), qacolsDF("DATASOURCE"), qacolsDF("ANSWER"))
      .withColumnRenamed("ANSWER", "SOURCE_SYSTEM_NAME")
      .filter("QUESTIONNUM = 'Q_SOURCE_SYSTEM'")
      .createOrReplaceTempView("srcsys_df")

    qacolsDF.select(qacolsDF("QUESTIONNUM"), qacolsDF("TRANSACTIONID"), qacolsDF("SOURCE"), qacolsDF("DATASOURCE"), qacolsDF("ANSWER"))
      .withColumnRenamed("ANSWER", "SUBSCRIPTION_ID")
      .filter("QUESTIONNUM = 'Q_TrialID'")
      .createOrReplaceTempView("subscr_df")

    qacolsDF.select(qacolsDF("QUESTIONNUM"), qacolsDF("TRANSACTIONID"), qacolsDF("SOURCE"), qacolsDF("DATASOURCE"), qacolsDF("ANSWER"))
      .withColumnRenamed("ANSWER", "KATAKANA_NAME")
      .filter("QUESTIONNUM = 'Q_KATAKANA'")
      .createOrReplaceTempView("kat_df")

    log.info("srcsys_df view...")
    AppProperties.SparkSession.table("srcsys_df").show()

    log.info("subscr_df view...")
    AppProperties.SparkSession.table("subscr_df").show()

    log.info("kat_df view...")
    AppProperties.SparkSession.table("kat_df").show()

    reg.createOrReplaceTempView("df")

    // Join Registration data with Question and Answer column pairings
    val reg_to_upload = AppProperties.SparkSession.sql(
      """select df.*,
        substring(ltrim(srcsys_df.SOURCE_SYSTEM_NAME), 0, 50) as SOURCE_SYSTEM_NAME,
        substring(ltrim(subscr_df.SUBSCRIPTION_ID), 0, 64) as SUBSCRIPTION_ID,
        substring(ltrim(kat_df.KATAKANA_NAME), 0, 300) as KATAKANA_NAME
        from df
        left join srcsys_df
        on df.TRANSACTIONID = srcsys_df.TRANSACTIONID
        and df.SOURCE = srcsys_df.SOURCE
        and df.DATASOURCE = srcsys_df.DATASOURCE
        left join subscr_df
        on df.TRANSACTIONID = subscr_df.TRANSACTIONID
        and df.SOURCE = subscr_df.SOURCE
        and df.DATASOURCE = subscr_df.DATASOURCE
        left join kat_df
        on df.TRANSACTIONID = kat_df.TRANSACTIONID
        and df.SOURCE = kat_df.SOURCE
        and df.DATASOURCE = kat_df.DATASOURCE""")

    val stg_connection = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, targetDB)
    stg_connection.setProperty("sslConnection", "true")

    log.info("reg_to_upload dataframe...")
    reg_to_upload.show()
    val reg_to_upload_final = reg_to_upload.persist()
    val targetRowCount = reg_to_upload_final.count().toInt

    log.info("Writing to STG_RAW_REGISTRATION table...")
    DataUtilities.runPreparedStatement(
      stg_connection,
      reg_to_upload_final,
      uplRegSql,
      reg_to_upload_final.columns,
      null,
      null,
      null)

    //DataUtilities.performInsertRecords(stg_connection, reg_to_upload_final, "MAP_STG.STG_RAW_REGISTRATION", reg_to_upload.columns)

    /* 2021-09-03 - SMSMKTMIP-510: We decided to switch from using an INSERT statement here to using a MERGE. This way we do not run
        the risk of duplicating records if this job is terminated in after it has written to the target tables but
        before setting the processed flags to 'Y'.
       2021-09-21 - We decided take similar action with the write to STG_RAW_REGISTRATION above. We changed the INSERT
        statement into a MERGE statement.
        */

    log.info("Writing to STG_RAW_ANSWER table...")
    DataUtilities.runPreparedStatement(
      stg_connection,
      ans,
      uplAnsSql,
      ans.columns,
      null,
      null,
      null)

    log.info("Show reg_to_upload_final...")
    val dfUpd = reg_to_upload_final.select("TRANSACTIONID").toDF()
    dfUpd.show(false)

    if (env != "DEV") {
    log.info("Updating processed flags in IWM.ETL_EXTRACT_CONTROL...")
    DataUtilities.runPreparedStatement(
      srcConn,
      dfUpd,
      "UPDATE IWM.ETL_EXTRACT_CONTROL SET PROCESSED_FLG = 'Y' WHERE TRANSACTIONID = ?",
      dfUpd.columns,
      null,
      null,
      null
    )
  }

    val mipDbEndpoint: String = stg_connection.getProperty(PropertyNames.EndPoint)
    log.info("Record row counts...")
    recordRowCounts(sourceRowCount, targetRowCount, mipDbEndpoint)

  }

  def main(args: Array[String]): Unit = {
    var isJobFailed: Boolean = false

    var sourceDB: String = null
    var targetDB: String = null
    var xtrRegSql: String = null
    var xtrAnsSql: String = null
    var uplRegSql: String = null
    var uplAnsSql: String = null
    var errStr: String = null
    var ENV: String = null

    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, 0, Constants.JobStarted, "", null, null)

      val sourceDBIndex = args.indexOf("--sourceDB")
      val targetDBIndex = args.indexOf("--targetDB")
      val xtrRegSqlIndex = args.indexOf("--xtrRegSql")
      val xtrAnsSqlIndex = args.indexOf("--xtrAnsSql")
      val uplRegSqlIndex = args.indexOf("--uplRegSql")
      val uplAnsSqlIndex = args.indexOf("--uplAnsSql")

      if (args.length % 2 == 0) {
        ENV = args(args.indexOf("--ENV") + 1)
      }

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

      if (xtrRegSqlIndex >= 0) {
        log.info(s"Arg 'xtrRegSql' found at index: $xtrRegSqlIndex")
        if (args.length > (xtrRegSqlIndex + 1)) {
          xtrRegSql = args(xtrRegSqlIndex + 1)
          log.info(s"Value for arg 'xtrRegSql' value: $xtrRegSql")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'xtrRegSql' could not be found. Please pass the value."
          )
        }
      }

      if (xtrAnsSqlIndex >= 0) {
        log.info(s"Arg 'xtrAnsSql' found at index: $xtrAnsSqlIndex")
        if (args.length > (xtrAnsSqlIndex + 1)) {
          xtrAnsSql = args(xtrAnsSqlIndex + 1)
          log.info(s"Value for arg 'xtrAnsSql' value: $xtrAnsSql")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'xtrAnsSql' could not be found. Please pass the value."
          )
        }
      }

      if (uplRegSqlIndex >= 0) {
        log.info(s"Arg 'uplRegSql' found at index: $uplRegSqlIndex")
        if (args.length > (uplRegSqlIndex + 1)) {
          uplRegSql = args(uplRegSqlIndex + 1)
          log.info(s"Value for arg 'uplRegSql' value: $uplRegSql")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'uplRegSql' could not be found. Please pass the value."
          )
        }
      }

      if (uplAnsSqlIndex >= 0) {
        log.info(s"Arg 'uplAnsSql' found at index: $uplAnsSqlIndex")
        if (args.length > (uplAnsSqlIndex + 1)) {
          uplAnsSql = args(uplAnsSqlIndex + 1)
          log.info(s"Value for arg 'uplAnsSql' value: $uplAnsSql")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'uplAnsSql' could not be found. Please pass the value."
          )
        }
      }


      runJobSequence(xtrRegSql, xtrAnsSql, uplRegSql, uplAnsSql, sourceDB, targetDB, ENV)

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

      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, 0,
        Constants.JobSucceeded, "", "", "")

      log.info(s"Completed Job => $jobClassName.")

    } catch {
      case e: Throwable => {
        isJobFailed = true
        e.printStackTrace
        errStr = e.getMessage + " - " + e.getCause
        log.error(errStr)

        DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, 0,
          Constants.JobFailed, errStr, null, null)
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

      log.info("jobSK = " + jobSK)

      // Now update this record with our source and target row counts.
      val sqlUpdateRowCounts = "UPDATE MAP_ETL.ETL_JOB_HIST SET " +
        "SOURCE_ROW_COUNT = '" + sourceRowCount + "', " +
        "TARGET_ROW_COUNT = '" + targetRowCount + "' " +
        "WHERE JOB_SK = '" + jobSK + "'"
      stmt.execute(sqlUpdateRowCounts)
      log.info("Done.  Row counts recorded")
    }
    catch {
      case e: Throwable => {
        log.info("Failed to write row counts to Job History table.")
        e.printStackTrace()
      }
    }
  }
}
