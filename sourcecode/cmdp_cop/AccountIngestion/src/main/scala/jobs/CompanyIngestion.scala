package jobs

import java.util.Date

import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}
import com.ibm.mkt.etlframework.data.DataUtilities



object CompanyIngestion extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")

  // Database Endpoint variables
  private var MIP_ENDPOINT = ""
  private var CMDP_ENDPOINT = ""
  private var companyPullSql = ""
  private var companyMergeSql = ""
  private var TIMESTAMP_OFFSET = 0
  private var dataframeCount = 0.0

  @throws(classOf[Exception])
  def runJobSequence = {
    log.info("runJobSequence started")
    // ETL Logic goes here

    // For tracking runtime of job
    val startTime = System.currentTimeMillis()
    val startDate = new Date(startTime * 1000L)
    log.info("start time: " + startDate)

    val MIPdbProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession,MIP_ENDPOINT)
    MIPdbProperties.setProperty("sslConnection", "true")
    val cmdpProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, CMDP_ENDPOINT)

    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 2000


    log.info("Getting Last Successful Run Time")
    val successfulTimestamp = DataUtilities.getLastSuccessfulRunTime(AppProperties.SparkSession, "CMDP_COP_to_MIP")
    var t = "1000-01-01-01.00.00.000000"
    if (!successfulTimestamp.isEmpty) {
      t = "\'" + successfulTimestamp("JOB_START_TIME") + "\'"
    }
    log.info(s"Last Successful Timestamp: $t")

    val formmated_date = s"(DATE($t) + $TIMESTAMP_OFFSET DAYS)"


    log.info("Pulling Company Data from CMDP based on timestamp.....")

    val companyData = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, cmdpProperties, companyPullSql.replace("?", formmated_date), Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    companyData.cache()
    dataframeCount = companyData.count()
    companyData.show()

    log.info("Merging Company Data into MIP Table....")

    DataUtilities.runPreparedStatement(
      MIPdbProperties,
      companyData,
      companyMergeSql,
      companyData.columns,
      null,
      null,
      null)

    log.info("runJobSequence ended.")
  }

  private def getArgs(args: Array[String]): Unit = {
    if (args.length % 2 == 0) {
      MIP_ENDPOINT = args(args.indexOf("--baseDB") + 1)
      CMDP_ENDPOINT = args(args.indexOf("--cmdpDB") + 1)
      companyPullSql = args(args.indexOf("--companyPullSql") + 1)
      companyMergeSql = args(args.indexOf("--companyMergeSql") + 1)
      TIMESTAMP_OFFSET = args(args.indexOf("--timestampOffset") + 1).toInt
    }
  }

  def main(args: Array[String]): Unit = {
    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      getArgs(args)
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

      runJobSequence

      // Log job status POST - DB
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
                                     AppProperties.CommonJobSeqCode,
                                    0,
                                     Constants.JobSucceeded,
                                    s"$jobClassName succeeded with $dataframeCount records pulled.", null, null)

      log.info(s"Completed Job => $jobClassName.")

    } catch {
      case e:Throwable=>  {
        e.printStackTrace
        log.error(e.getMessage +" - "+e.getCause)
        DataUtilities.recordJobHistory(AppProperties.SparkSession,
                                       AppProperties.CommonJobSeqCode,
                                       0,
                                       Constants.JobFailed,
                                       e.getMessage +" - "+ e.getCause, null, null)
      }
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }

}

