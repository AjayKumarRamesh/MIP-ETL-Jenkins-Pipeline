package jobs.staging

import java.util.Date

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}

object CDS_CMDP_Staging extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var isJobFailed = false

  // Database Endpoint variables
  private var MIP_ENDPOINT = ""
  private var CMDP_ENDPOINT = ""

  // Job arguments
  private var utSQL = ""
  private var geoSQL = ""
  private var dimURLSQL = ""

  @throws(classOf[Exception])
  def runJobSequence_updateStagingTables(): Unit = { // NOSONAR


    // For tracking runtime of job
    val startTime = System.currentTimeMillis()
    val startDate = new Date(startTime * 1000L)
    log.info("start time: " + startDate)

    val MIPdbProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, MIP_ENDPOINT)
    MIPdbProperties.setProperty("sslConnection", "true")
    val cmdpProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, CMDP_ENDPOINT)
    cmdpProperties.setProperty("sslConnection", "true")

    // Needed to avoid database lockouts from multiple partitions trying to create connections at the same time.
    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 2000

    log.info("Pulling CMDP staging data.....")
    log.info("Pulling UT data..")
    val utDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, cmdpProperties, utSQL, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    log.info("Pulling Geo data..")
    val geoDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, cmdpProperties, geoSQL, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    log.info("Pulling dimURL data..")
    val dimDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, cmdpProperties, dimURLSQL, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")

    log.info("Truncate and Loading CMDP staging data.....")

    log.info("Truncate and load UT Staging table..")
    DataUtilities.saveDataframeToDB(AppProperties.SparkSession, MIPdbProperties, utDF, "MAP_STG.STG_CMDP_REF_UT_BRAND", Constants.PersistUsingSourceColumns, Constants.SaveModeOverwrite)
    log.info("Truncate and load Geo Staging table..")
    DataUtilities.saveDataframeToDB(AppProperties.SparkSession, MIPdbProperties, geoDF, "MAP_STG.STG_CMDP_REF_IBM_GBL_GEO", Constants.PersistUsingSourceColumns, Constants.SaveModeOverwrite)
    log.info("Truncate and load dimURL Staging table..")
    DataUtilities.saveDataframeToDB(AppProperties.SparkSession, MIPdbProperties, dimDF, "MAP_STG.STG_CMDP_DIM_URL", Constants.PersistUsingSourceColumns , Constants.SaveModeOverwrite)
    log.info("Finished.")
    val totalTime: Long = (System.currentTimeMillis() - startTime) / (1000 * 60)
    log.info("Total time: " + totalTime + "  minutes.")
  }


  private def getArgs(args: Array[String]): Unit = {

    if (args.length % 2 == 0) {
      MIP_ENDPOINT = args(args.indexOf("--baseDB") + 1)
      CMDP_ENDPOINT = args(args.indexOf("--cmdpDB") + 1)
      utSQL = args(args.indexOf("--utSql") + 1)
      geoSQL = args(args.indexOf("--geoSql") + 1)
      dimURLSQL = args(args.indexOf("--dimURLSql") + 1)
    }
  }


  def main(args: Array[String]): Unit = {
    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      // Log job status START - DB
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobStarted,
        s"$jobClassName Started", null, null)


      log.info("Starting ETL...")
      getArgs(args)
      runJobSequence_updateStagingTables()
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
        s"Completed Job => $jobClassName.", null, null)

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
