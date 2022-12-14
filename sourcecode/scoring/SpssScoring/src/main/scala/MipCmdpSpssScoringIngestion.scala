import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants}

import scala.sys.exit

//This scala job will prepare data to be scored and ingest it from MIP Responder scoring view to CMDP(ACSRSPS0)  Responder scoring table.
object MipCmdpSpssScoringIngestion extends MIPUtilities {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var bException: Boolean = false

  @throws(classOf[Exception])
  def runJobSequence() = {
    log.info("runJobSequence started")
    val sparkSession = AppProperties.SparkSession // NOSONAR
    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 1000


    try {
      var crudMask = Constants.CDCActionMaskInsert | Constants.CDCActionMaskBuildSuccessRecsLogs
      var dfToProcessRecsKeysWithStatusV1 = processSubSequence("MIP_CMDP", crudMask, "D", "E")
      if (dfToProcessRecsKeysWithStatusV1 != null) {
        processRecStatusUpdate("CMDP_MIP_STATUS_UPDATE", dfToProcessRecsKeysWithStatusV1)
      }
      crudMask = Constants.CDCActionMaskLogicalDelete | Constants.CDCActionMaskBuildSuccessRecsLogs
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        println("MipCmdpSpssScoringIngestion catch block")
        bException = true
      }
    } finally { // NOSONAR
    } // NOSONAR

    log.info("runJobSequence ended.")
  }

  // Args: --fromFromDataSourceCode, ToDataSourceCode, StartDate, EndDate
  def main(args: Array[String]): Unit = {

    args.foreach(println(_))

    try {
      this.initializeFramework(args)
      log.info("Initialization started")

      if (args.indexOf("--jobseq") == -1) log.error("Job  Sequence code(s) not provided")

      log.info("Initialization completed.")
      log.info(s"Starting ETL Job => $jobClassName....")

      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "",
        Constants.JobStarted)
      runJobSequence()
      log.info(s"Completed Job => $jobClassName.")
    } catch {
      case e: Throwable => {
        e.printStackTrace
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
      }
    } finally {
      DataUtilities.writeLogsToCOS(AppProperties.SparkSession, List(jobSequenceLogFileName))
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "",
        if (bException) Constants.JobFailed else Constants.JobSucceeded)
      if (bException)
        exit(1)
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }
}
