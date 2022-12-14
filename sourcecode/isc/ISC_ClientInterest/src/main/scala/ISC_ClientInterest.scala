import java.util.Date

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}

object ISC_ClientInterest extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var isJobFailed = false
  var jobSeq = ""

  // Database Endpoint variables
  private var MIP_ENDPOINT = ""
  private var CEDP_ENDPOINT = ""

  // Env variables for runtime
  private var iscPullSql = ""
  private var iscMergeSql = ""
  private var TIMESTAMP_OFFSET = ""
  private var dataframeCount = 0.0
  private var defaultTimestamp = ""


  @throws(classOf[Exception])
  def runJobSequence(): Unit = {
    log.info("runJobSequence started")

    // For tracking runtime of job
    val startTime = System.currentTimeMillis()
    val startDate = new Date(startTime * 1000L)
    log.info("start time: " + startDate)

    val MIPdbProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession,MIP_ENDPOINT)
    MIPdbProperties.setProperty("sslConnection", "true")

    val cedpDBProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession,CEDP_ENDPOINT)
    cedpDBProperties.setProperty("sslConnection", "true")

    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 2000

    log.info("Setting current timestamp to default timestamp: " + defaultTimestamp)
    var t = "\'" + defaultTimestamp + "\'"
    log.info("Getting Last Successful Run Time for JobSeq " + jobSeq)
    val successfulTimestamp = DataUtilities.getLastSuccessfulRunTime(AppProperties.SparkSession, jobSeq)
    // If start and end are equal, that means there's no job history and need to use default.
    log.info(successfulTimestamp.seq)
    if (!successfulTimestamp.isEmpty && (successfulTimestamp("JOB_START_TIME") != successfulTimestamp("JOB_END_TIME"))) {
      log.info("Start time and end time not the same, setting timestamp to start time.")
      t = "\'" + successfulTimestamp("JOB_START_TIME") + "\'"
    } else {
      log.info("No job history found, keeping timestamp default value.")
    }
    log.info(s"Last Successful Timestamp: $t")

    val formmated_date = s"(DATE($t) + $TIMESTAMP_OFFSET DAYS)"

    log.info("Pulling ISC Client Interest data from CEDP based on timestamp....")

    val iscData = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, cedpDBProperties, iscPullSql.replace("?", formmated_date), Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    iscData.cache()
    dataframeCount = iscData.count()
    iscData.show()

    log.info("Merging data into ISC Table in MIP....")

    DataUtilities.runPreparedStatement(
      MIPdbProperties,
      iscData,
      iscMergeSql,
      iscData.columns,
      null,
      null,
      null)

    log.info("runJobSequence ended.")
  }


  private def getArgs(args: Array[String]): Unit = {

    if (args.length % 2 == 0) {
      jobSeq = args(args.indexOf("--jobseq") + 1)
      MIP_ENDPOINT = args(args.indexOf("--baseDB") + 1)
      CEDP_ENDPOINT = args(args.indexOf("--cedpDB") + 1)
      iscPullSql = args(args.indexOf("--iscPullSql") + 1)
      TIMESTAMP_OFFSET = args(args.indexOf("--offset") + 1)
      iscMergeSql = args(args.indexOf("--iscMergeSql") + 1)
      defaultTimestamp = args(args.indexOf("--defaultTimestamp") + 1)
    } else {
      throw new IllegalArgumentException(
        "Uneven amount of argument key-value pairs provided."
      )
    }
  }

      def main(args: Array[String]): Unit = {
    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      // Log job status START - DB
      // log.info(s"CommonDBConnProperties => ${this.CommonDBConProperties}")
      // log.info(s"Log to JobHistoryLogTable => ${AppProperties.JobHistoryLogTable}")
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobStarted,
        s"$jobClassName Started", null, null)


      log.info("Starting ETL...")
      getArgs(args)
      runJobSequence()
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

      // Log job status POST - DB
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobSucceeded,
        s"Completed Job => $jobClassName.  Records pulled from CEDP = $dataframeCount", null, null)

      log.info(s"Completed Job => $jobClassName.")

    } catch {
      case e: Throwable =>
        isJobFailed = true
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        DataUtilities.recordJobHistory(AppProperties.SparkSession,
          AppProperties.CommonJobSeqCode,
          0,
          Constants.JobFailed,
          e.getMessage + " - " + e.getCause, null, null)
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")

      if (isJobFailed) {
        System.exit(1)
      }
    }
  }
}
