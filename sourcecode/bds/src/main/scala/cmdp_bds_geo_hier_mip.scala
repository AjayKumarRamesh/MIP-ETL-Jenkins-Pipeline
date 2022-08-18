import com.ibm.mkt.etlframework.audit.CRUDMetrics
import com.ibm.mkt.etlframework.config.JobSequenceService
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants}

import java.util.Properties
import scala.sys.exit

//This scala job will prepare data to be scored and ingest it from MIP Responder scoring view to CMDP(ACSRSPS0)  Responder scoring table.
object cmdp_bds_geo_hier_mip extends MIPUtilities {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var bException: Boolean = false
  var CmdpGeoSrc: String = null
  var MipStgTgt: String = null

  def main(args: Array[String]): Unit = {

    args.foreach(println(_))
    CmdpGeoSrc = args(args.indexOf("--CmdpGeoSrc") + 1)
    MipStgTgt = args(args.indexOf("--MipStgTgt") + 1)


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
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "",
        if (bException) Constants.JobFailed else Constants.JobSucceeded)
      if (bException)
        exit(1)
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }

  @throws(classOf[Exception])
  def runJobSequence() = {
    log.info("runJobSequence started")
    val sparkSession = AppProperties.SparkSession
    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 1000

    var retCRUDMetrics: CRUDMetrics = null
    var srcConnectionInfo: Properties = null
    var tgtConnectionInfo: Properties = null

    val connTgt = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, MipStgTgt)

    truncateTable(connTgt,"MAP_STG.STG_CMDP_RAW_COUNTRY")
    log.info("======Truncate table is complete======")

    val Cmdpconn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, CmdpGeoSrc) //connecting to source MIP database
    val dfCmdp = DataUtilities.readDataByPartitioningType( //creating dataframe for Source extract operation
      AppProperties.SparkSession,
      Cmdpconn," SELECT URN_BBG_IBM_GBL_GEO,CTRY,CTRY_SEQ,SRC_CTRY_CD,SRC_CTRY_TYPE,CTRY_NAME,IBM_GBL_IOT_CD,IBM_GBL_IOT_DSCR,IBM_GBL_IMT_CD," +
        "IBM_GBL_IMT_DSCR,IBM_GBL_RGN_CD,IBM_GBL_RGN_DSCR,URN_BBG_CTRY,MOD(ROW_NUMBER() OVER(ORDER BY URN_BBG_IBM_GBL_GEO), 4) AS PARTCOL FROM V2REFR2.V_REF_IBM_GBL_GEO A " +
        "WHERE CTRY_SEQ = (SELECT min(CTRY_SEQ) FROM V2REFR2.V_REF_IBM_GBL_GEO  WHERE CTRY = A.CTRY AND IBM_GBL_IMT_CD = A.IBM_GBL_IMT_CD GROUP BY CTRY ,IBM_GBL_IMT_CD )", //passing argument to use extract sql
      Constants.PartitionTypeByColumn,
      null,
      "PARTCOL"
    )
    log.info(s"${dfCmdp.count()} row(s) extracted")
    // dfCmdp.show(numRows = 100, truncate = false) //logging purpose
    // log.info(s"Num Partitions: ${dfCmdp.rdd.getNumPartitions}")
    //val connTgt = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, MipStgTgt) //connecting to target GRP database
    val dfTgt = dfCmdp.drop("PARTCOL") //creating dataframe for merge operation
    // processedCount = dfTgt.count()
    DataUtilities.performInsertRecords(
      connTgt,
      dfTgt,
      "MAP_STG.STG_CMDP_RAW_COUNTRY",
      dfTgt.columns)


    try {
      var crudMask = Constants.CDCActionMaskUpdate | Constants.CDCActionMaskInsert | Constants.CDCActionMaskLogicalDelete
      AppProperties.CommonJobSubSeqCode = "GEO_CMDP_MIP"
      val jobConfig = JobSequenceService.getJobSequence(AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode)
      srcConnectionInfo = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, jobConfig.fromDataSourceCode)
      tgtConnectionInfo = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, jobConfig.toDataSourceCode)

      var incrementalRecordsCondition: DataUtilities.IncrementalRecordsCondition =
        new DataUtilities.IncrementalRecordsCondition(jobConfig.newRecordsIdentifier, jobConfig.updateRecordsIdentifier,
          jobConfig.deleteRecordsIdentifier)
      retCRUDMetrics = DataUtilities.performCDCUsingTableCompare(sparkSession, srcConnectionInfo, tgtConnectionInfo,jobConfig.fromDataObjectName,
        jobConfig.toDataObjectName,jobConfig.uniqueRecordIdentifiers.split(","),jobConfig.targetSourceColumnMapping,
        jobConfig.crudColumnActionJson, true, true, Constants.DeleteLogical, null,crudMask)


    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
      }
    } finally {
    }
    log.info("runJobSequence ended.")
  }



}


