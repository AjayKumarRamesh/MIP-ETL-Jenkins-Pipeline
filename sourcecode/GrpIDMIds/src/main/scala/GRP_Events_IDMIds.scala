import com.ibm.mkt.etlframework.audit.JobRunArgs
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}

object GRP_Events_IDMIds extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var errstr: String = null
  var dbExtrSource: String = null
  var dbTgtSource: String = null
  var processedCount: Long = 0

  def main(args: Array[String]): Unit = {
    var isJobFailed: Boolean = false

    var xtrSql: String = null //creating command line arguments for extract and upload sql
    var uplSql: String = null
    dbExtrSource = args(args.indexOf("--dbExtrSource") + 1)
    dbTgtSource = args(args.indexOf("--dbTgtSource") + 1)
    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      // Log job status START - DB
      // log.info(s"CommonDBConnProperties => ${this.CommonDBConProperties}")
      // log.info(s"Log to JobHistoryLogTable => ${AppProperties.JobHistoryLogTable}")
      //DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobStarted, "GOOD LUCK", null, null)
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobStarted)


      // TODO: ETL Logic goes here...
      log.info("ETL logic goes here...")
      val xtrSqlIndex = args.indexOf("--xtrSql") //creating loop variables to takeup cmd line args
      val uplSqlIndex = args.indexOf("--uplSql")
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

      if (uplSqlIndex >= 0) {
        log.info(s"Arg 'xtrSql' found at index: $uplSqlIndex")
        if (args.length > (uplSqlIndex + 1)) {
          uplSql = args(uplSqlIndex + 1)
          log.info(s"Value for arg 'xtrSql' value: $uplSql")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'xtrSql' could not be found. Please pass the value."
          )
        }
      }
      runJobSequence(xtrSql, uplSql)
      /* Note: You have access to the following instances:
        - CommonDBConProperties (connection properties to connect to App Config DB)
        - commonJobSeqCode (job sequence code)
        - sparkSession (spark session instance)

        At this point, you should have access to Common functions
        e.g: this.getDataSourceDetails(sparkSession, dataSourceCode = "ISAP")

        If you need to extract and save/write data, then use the Object: DataUtilities
        This object is a facade to all functions available.
        e.g: DataUtilities.readDataWithColumnPartitioning(...)
      */


      //     logging job run statistics into Job history table
      // Log job status POST - DB
      //DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobSucceeded, "Job completed without any error", null, "Total Count of Records Processed "+ processedCount)
      val jobrun = new JobRunArgs
      jobrun.jobMetrics = "Total Count of Records Processed " + processedCount
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobSucceeded, jobrun)
      log.info(s"Completed Job => $jobClassName.") //logging when job is succeeded

    } catch {
      case e: Throwable => {
        isJobFailed = true
        e.printStackTrace
        log.error(e.getMessage + " - " + e.getCause)
        // DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobFailed, errstr, null, null)
        DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobFailed)
      }
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...") //cleanup the framework and shows exit code 1 in the log
      if (isJobFailed) { // when the job is failed
        System.exit(1)
      }
    }
  }


  @throws(classOf[Exception])
  def runJobSequence(xtrSql: String, uplSql: String) = {
    log.info("runJobSequence started")
    // ETL Logic goes here
    /* Note: You have access to the following instances:
        - CommonDBConProperties (connection properties to connect to App Config DB)
        - commonJobSeqCode (job sequence code)
        - sparkSession (spark session instance)

        At this point, you should have access to Common functions
        e.g: this.getDataSourceDetails(sparkSession, dataSourceCode = "ISAP")

        If you need to extract and save/write data, then use the Object: DataUtilities
        This object is a facade to all functions available.
        e.g: DataUtilities.readDataWithColumnPartitioning(...)
    */
    val conn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbExtrSource) //connecting to source MIP database
    val df = DataUtilities.readDataByPartitioningType( //creating dataframe for Source extract operation
      AppProperties.SparkSession,
      conn,
      xtrSql, //passing argument to use extract sql
      Constants.PartitionTypeByColumn,
      null,
      "PARTCOL"
    )
    log.info(s"${df.count()} row(s) extracted")
    df.show(numRows = 100, truncate = false) //logging purpose
    log.info(s"Num Partitions: ${df.rdd.getNumPartitions}")
    val connTgt = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbTgtSource) //connecting to target GRP database
    val dfTgt = df.drop("PARTCOL") //creating dataframe for merge operation
    processedCount = dfTgt.count()
    DataUtilities.runPreparedStatement(
      connTgt,
      dfTgt,
      uplSql,
      dfTgt.columns, //passing argument to use upload sql
      null, null, "merge"
    )
    val dfUpd = df.select("IORDNUM") //creating dataframe with column that can be used to join
    DataUtilities.runPreparedStatement(
      conn,
      dfUpd,
      "UPDATE MAP_CORE.MCT_EVENT_IDM_XREF SET PROCESSED_FLG = 'Y' WHERE IORDNUM = ?",
      dfUpd.columns,
      null, null, "update")

  }

}
