package jobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}

object SampleJob2 extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")

  @throws(classOf[Exception])
  def runJobSequence = {
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
    val srcConn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, "ETL_DEV")
    val srcDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession,
      srcConn,
      "SELECT PROCESS, LEVEL, IS_ACTIVE, PROCEDURE_NAME, DEDUPE, DROPNA, " +
        "MOD(ROW_NUMBER() OVER (ORDER BY 1), 4) AS PARTCOL FROM ETL_MAP.IND_COMP_CONF",
      Constants.PartitionTypeByColumn,
      null,
      "PARTCOL")
    val rc = srcDF.count()
    log.info(s"$rc row(s) extracted")
    val tgtDF = srcDF.drop("PARTCOL")
    tgtDF.show(numRows = 100, truncate = false)
    val np = tgtDF.rdd.getNumPartitions
    log.info(s"Num Partitions: $np")
    log.info("runJobSequence ended.")
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
        "GOOD LUCK")


      // TODO: ETL Logic goes here...
      log.info("ETL logic goes here...")
      runJobSequence
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
        "")

      log.info(s"Completed Job => $jobClassName.")

    } catch {
      case e: Throwable => {
        e.printStackTrace
        log.error(e.getMessage + " - " + e.getCause)
        DataUtilities.recordJobHistory(AppProperties.SparkSession,
          AppProperties.CommonJobSeqCode,
          0,
          Constants.JobFailed,
          s"e.getMessage - e.getCause")
      }
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }

}
