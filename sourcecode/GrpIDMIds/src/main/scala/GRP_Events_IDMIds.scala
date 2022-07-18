import com.ibm.mkt.etlframework.audit.JobRunArgs
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}

object GRP_Events_IDMIds extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var errstr: String = null
  var dbExtrSource: String = null
  var dbTgtSource: String = null
  var mergedCount: Long = 0
  var sourceCount: Long = 0
  var errorCount: Long = 0

  def main(args: Array[String]): Unit = {
    var isJobFailed: Boolean = false

    val xtrSql: String =
      """SELECT
        |	IORDNUM,
        |	IDM_PERSON_EID,
        |	IDM_ORG_EID,
        |	EMAIL_MEDIA_ID,
        |	MOD(ROW_NUMBER() OVER(ORDER BY IORDNUM), 4) AS PARTCOL
        |FROM MAP_CORE.MCT_EVENT_IDM_XREF
        |WHERE PROCESSED_FLG = 'N'""".stripMargin

    val uplSql: String =
      """MERGE INTO GRP_MAP.IORDXREF t
        |USING TABLE ( VALUES (?,?,?,?) ) as v (IORDNUM , IDM_PERSON_EID,IDM_ORG_EID,EMAIL_MEDIA_ID)
        |	ON t.IORDNUM = v.IORDNUM
        |WHEN MATCHED THEN
        |	UPDATE SET t.IDM_PERSON_EID = v.IDM_PERSON_EID, t.IDM_ORG_EID = v.IDM_ORG_EID,t.EMAIL_MEDIA_ID = v.EMAIL_MEDIA_ID
        |WHEN NOT MATCHED THEN
        |	INSERT (IORDNUM, IDM_PERSON_EID, IDM_ORG_EID,EMAIL_MEDIA_ID) VALUES (v.IORDNUM, v.IDM_PERSON_EID, v.IDM_ORG_EID,v.EMAIL_MEDIA_ID)""".stripMargin

    dbExtrSource = args(args.indexOf("--dbExtrSource") + 1)
    dbTgtSource = args(args.indexOf("--dbTgtSource") + 1)
    try {
      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobStarted)

      runJobSequence(xtrSql, uplSql)

      val jobrun = new JobRunArgs
      jobrun.jobMetrics = s"Source Count: $sourceCount, Merged Count: $mergedCount, Error Count: $errorCount"

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
    val sourceConn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbExtrSource) //connecting to source MIP database
    val extractDF = DataUtilities.readDataByPartitioningType( //creating dataframe for Source extract operation
      AppProperties.SparkSession,
      sourceConn,
      xtrSql, //passing argument to use extract sql
      Constants.PartitionTypeByColumn,
      null,
      "PARTCOL"
    )
    sourceCount = extractDF.count()
    log.info(s"$sourceCount row(s) extracted")
    extractDF.show(numRows = 100, truncate = false) //logging purpose
    log.info(s"Num Partitions: ${extractDF.rdd.getNumPartitions}")
    val targetConn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbTgtSource) //connecting to target GRP database
    val targetDF = extractDF.drop("PARTCOL") //creating dataframe for merge operation
    DataUtilities.runPreparedStatement(
      targetConn,
      targetDF,
      uplSql,
      targetDF.columns, //passing argument to use upload sql
      null, null, "merge"
    )

    val srcCompareDF = targetDF.select("IORDNUM")
    val iordNums = srcCompareDF.collect().map(row => row.getString(0)).mkString("','")
    val updatedSql =
      s"""SELECT
         |	IORDNUM
         |FROM GRP_MAP.IORDXREF
         |WHERE IORDNUM IN ('$iordNums')""".stripMargin
    //Reading target table
    val targetCompareDF = AppProperties.SparkSession.read.jdbc(targetConn.getProperty(PropertyNames.EndPoint), updatedSql, targetConn)
    val diffDF = srcCompareDF.except(targetCompareDF)
    //val updateTargetDF = srcCompareDF.except(diffDF)
    mergedCount = targetCompareDF.count()
    errorCount = diffDF.count()

    if(mergedCount > 0 ) {
      DataUtilities.runPreparedStatement(
        sourceConn,
        targetCompareDF,
        "UPDATE MAP_CORE.MCT_EVENT_IDM_XREF SET PROCESSED_FLG = 'Y' WHERE IORDNUM = ?",
        targetCompareDF.columns,
        Array(0), null, "update")
    }

    if(errorCount > 0 ) {
      DataUtilities.runPreparedStatement(
        sourceConn,
        diffDF,
        "UPDATE MAP_CORE.MCT_EVENT_IDM_XREF SET PROCESSED_FLG = 'E' WHERE IORDNUM = ?",
        diffDF.columns,
        Array(0), null, "update")
    }

  }



}

