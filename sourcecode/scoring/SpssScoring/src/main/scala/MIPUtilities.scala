import com.ibm.db2.jcc.am.SqlInvalidAuthorizationSpecException
import com.ibm.mkt.etlframework.audit.{CRUDMetrics, JobRunArgs}
import com.ibm.mkt.etlframework.config.JobSequenceService
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager}
import java.io.File
import java.util.Properties
import util.control.Breaks._

trait MIPUtilities extends ETLFrameWork {
  var isSuccess = true
  val retryForErrorCode = "ERRORCODE=-4214"
  val retryCount: Integer = 2
  def processSubSequenceWithCutOffTS(subSequenceCode: String, crudMask: Int, successFlag: String, errorFlag: String,
                                     cutOffTimeStamp: String = null): DataFrame = {
    log.info("runJobSequence-1 started")
    val sparkSession = AppProperties.SparkSession

    val jobDetail: JobRunArgs = new JobRunArgs()

    var bException: Boolean = false
    var retCRUDMetrics: CRUDMetrics = null
    var srcConnectionInfo: Properties = null
    var tgtConnectionInfo: Properties = null
    var dfToProcessRecsKeysWithStatus: DataFrame = null
    try {

      // Process Subsequence - code CMDP_MIP
      AppProperties.CommonJobSubSeqCode = subSequenceCode
      DataUtilities.recordJobHistory(sparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode,
        Constants.JobStarted)
      log.info(s"Current Sub Sequence code ->${AppProperties.CommonJobSubSeqCode}")
      val jobConfig = JobSequenceService.getJobSequence(AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode)
      srcConnectionInfo = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, jobConfig.fromDataSourceCode)
      tgtConnectionInfo = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, jobConfig.toDataSourceCode)

      //Build conditions
      var newRecsCondition: String = null
      var updateRecsCondition: String = null
      var deleteRecsCondition: String = null
      if (jobConfig.newRecordsIdentifier != null && jobConfig.newRecordsIdentifier.nonEmpty)
        newRecsCondition = jobConfig.newRecordsIdentifier.replaceAll("#CUTOFF_TIMESTAMP#", cutOffTimeStamp)
      if (jobConfig.updateRecordsIdentifier != null && jobConfig.updateRecordsIdentifier.nonEmpty)
        updateRecsCondition = jobConfig.updateRecordsIdentifier.replaceAll("#CUTOFF_TIMESTAMP#", cutOffTimeStamp)
      if (jobConfig.deleteRecordsIdentifier != null && jobConfig.deleteRecordsIdentifier.nonEmpty)
        deleteRecsCondition = jobConfig.deleteRecordsIdentifier.replaceAll("#CUTOFF_TIMESTAMP#", cutOffTimeStamp)


      val incrementalRecordsConditions = new DataUtilities.IncrementalRecordsCondition(newRecsCondition,
        updateRecsCondition, deleteRecsCondition)

      retCRUDMetrics = DataUtilities.performCDCUsingConditions(sparkSession, srcConnectionInfo, tgtConnectionInfo,
        jobConfig.fromDataObjectName,
        jobConfig.toDataObjectName, jobConfig.uniqueRecordIdentifiers, jobConfig.targetSourceColumnMapping,
        jobConfig.crudColumnActionJson, incrementalRecordsConditions, jobConfig.trimTargetTextColumns,
        true, Constants.DeleteIgnore, Constants.PartitionTypeByNone, null,
        crudMask)
      dfToProcessRecsKeysWithStatus = getCDCProcessedRecKeysWithStatus(sparkSession, retCRUDMetrics,
        successFlag, errorFlag, crudMask)

      jobDetail.crudMetrics = retCRUDMetrics.toCompactString()
      log.info(s"CRUD Metrics -> ${retCRUDMetrics.toString()}")
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
      }
    } finally {
      //Write CRUD error logs to COS
      if (retCRUDMetrics != null && retCRUDMetrics.logFilesList.nonEmpty) {
        log.info(retCRUDMetrics.logFilesList.toString())
        DataUtilities.writeLogsToCOS(sparkSession, retCRUDMetrics.logFilesList.toList)
      }
      DataUtilities.recordJobHistory(sparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode,
        if (bException) Constants.JobFailed else Constants.JobSucceeded, jobDetail)
    }
    log.info(s"Job sub sequence ${AppProperties.CommonJobSubSeqCode} done")
    dfToProcessRecsKeysWithStatus
  }


  def processSubSequence(subSequenceCode: String, crudMask: Int, successFlag: String, errorFlag: String): DataFrame = {
    log.info("runJobSequence-1 started")
    val sparkSession = AppProperties.SparkSession
    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 1000

    val jobDetail: JobRunArgs = new JobRunArgs()

    var bException: Boolean = false
    var retCRUDMetrics: CRUDMetrics = null
    var srcConnectionInfo: Properties = null
    var tgtConnectionInfo: Properties = null
    var dfToProcessRecsKeysWithStatus: DataFrame = null
    var exception: Throwable = null

    try {
      // Process Subsequence - code CMDP_MIP
      AppProperties.CommonJobSubSeqCode = subSequenceCode
      DataUtilities.recordJobHistory(sparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode,
        Constants.JobStarted)
      log.info(s"Current Sub Sequence code ->${AppProperties.CommonJobSubSeqCode}")
      val jobConfig = JobSequenceService.getJobSequence(AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode)
      srcConnectionInfo = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, jobConfig.fromDataSourceCode)
      tgtConnectionInfo = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, jobConfig.toDataSourceCode)
      //Retry dbconnection loop logic

      breakable {
        for (retryIndex <- 1 to retryCount) {
          try {
            log.info("executing processSubSequence: performCDCUsingConditions() for retry index# {}", retryIndex)
            retCRUDMetrics = DataUtilities.performCDCUsingConditions(sparkSession, srcConnectionInfo, tgtConnectionInfo, jobConfig,
              true, crudMask, null)
            dfToProcessRecsKeysWithStatus = getCDCProcessedRecKeysWithStatusV1(sparkSession, retCRUDMetrics,
              successFlag, errorFlag, crudMask)
            jobDetail.crudMetrics = retCRUDMetrics.toCompactString()
            log.info("processSubSequence: performCDCUsingConditions - is Successful with retry index# {}", retryIndex)
            isSuccess = true
          } catch {
            case e: Throwable =>
              e.printStackTrace()
              log.error(s"Exception => ${e.getMessage} -  ${e.getCause}")
              println(s"breakable => $e")
              println(s" Inside if SQL exception => ${e.getMessage}")
              if(e.getMessage.contains(retryForErrorCode) || e.getMessage.equals(null)){
                isSuccess = false
              }
              else {
                isSuccess = true
              }
          } finally {
            if (isSuccess) break
            else {
              val sleepTime: Long = 10000
              try {
                log.info("sleep for {} sec", sleepTime)
                Thread.sleep(sleepTime)
              } catch {
                case i: InterruptedException =>
                  log.error("Error while sleeping...."+i.getMessage +" - "+i.getCause)
              }
                 }
                     } //finally
        }
      }
      if (retCRUDMetrics!= null) log.info(s"CRUD Metrics -> ${retCRUDMetrics.toString()}")

    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        println("ProcessSubseq outside catch block")
        exception = e
        bException = true
      }
    } finally {
      //Write CRUD error logs to COS
      if (retCRUDMetrics != null && retCRUDMetrics.logFilesList.nonEmpty) {
        log.info(retCRUDMetrics.logFilesList.toString())
        DataUtilities.writeLogsToCOS(AppProperties.SparkSession, retCRUDMetrics.logFilesList)
      }

      DataUtilities.recordJobHistory(sparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode,
        if (bException) Constants.JobFailed else Constants.JobSucceeded, jobDetail)
      if (bException) throw exception
    }
    log.info(s"Job sub sequence ${AppProperties.CommonJobSubSeqCode} done")
    dfToProcessRecsKeysWithStatus
  }

  def processRecStatusUpdate(subSequenceCode: String, dfToProcessRecsKeysWithStatus: DataFrame): Unit = {
    log.info("runJobSequence-2 started")
    val sparkSession = AppProperties.SparkSession

    val jobDetail: JobRunArgs = new JobRunArgs()

    var bException: Boolean = false
    var retCRUDMetrics: CRUDMetrics = null
    var exception: Throwable = null

    try {
      //Update source tables with status code
      AppProperties.CommonJobSubSeqCode = subSequenceCode

      val jobConfigStatusUpdate = JobSequenceService.getJobSequence(AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode)
      val tgtConnectionInfo = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, jobConfigStatusUpdate.toDataSourceCode)
      //Update status in the source table
      //RETRY db connection logic
      var retCRUDMetricsUpdate:CRUDMetrics = null
      breakable {
        for (retryIndex <- 1 to retryCount) {
          try {
            log.info("executing processRecStatusUpdate: performCDCUsingConditions() for retry index# {}", retryIndex)
            retCRUDMetricsUpdate = DataUtilities.performCDCUsingConditions(sparkSession, null,
              tgtConnectionInfo, jobConfigStatusUpdate, true, Constants.CDCActionMaskLogicalDelete,
              dfToProcessRecsKeysWithStatus)
            log.info("processRecStatusUpdate: performCDCUsingConditions - is Successful with retry index# {}", retryIndex)
            isSuccess = true
          } catch {
            case e: Throwable =>
              log.error(s"Exception => ${e.getMessage} -  ${e.getCause}")
              if(e.getMessage.contains(retryForErrorCode)){
                isSuccess = false
              }
              else {
                isSuccess = true
              }
          } finally {
            if (isSuccess) break
            else {
              val sleepTime: Long = 10000
              try {
                log.info("sleep for {} sec", sleepTime)
                Thread.sleep(sleepTime)
              } catch {
                case i: InterruptedException =>
                  log.error("Error while sleeping...."+i.getMessage +" - "+i.getCause)
              }
            }
          }
        }
      }

      jobDetail.crudMetrics = retCRUDMetricsUpdate.toCompactString()
      log.info(s"CRUD Metrics -> ${retCRUDMetricsUpdate.toString()}")
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        exception = e
        bException = true
      }
    } finally {
      //Write CRUD error logs to COS
      if (retCRUDMetrics != null && retCRUDMetrics.logFilesList.nonEmpty) {
        log.info(retCRUDMetrics.logFilesList.toString())
        DataUtilities.writeLogsToCOS(AppProperties.SparkSession, retCRUDMetrics.logFilesList)
      }
      DataUtilities.recordJobHistory(sparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSubSeqCode,
        if (bException) Constants.JobFailed else Constants.JobSucceeded, jobDetail)
      if (bException) throw exception

    }
    log.info(s"Job sub sequence ${AppProperties.CommonJobSubSeqCode} done")
  }

  def getCDCProcessedRecKeysWithStatusV1(sparkSession: SparkSession, retCRUDMetrics: CRUDMetrics, successFlag: Any,
                                         errorFlag: Any, cdcActionMask: Int): DataFrame = {
    log.info("getCDCProcessedRecordStatus")

    var bException: Boolean = false
    var dfCrudErrorRecsKeys: DataFrame = null
    var dfSuccessRecsKeys: DataFrame = null
    var dfToBeProcessedRecWithStatus: DataFrame = null
    val errColNames = (s"${retCRUDMetrics.tgtKeyColNames},${Constants.CDCErrDescColName}").split(",").map(_.toString)
    val successColNames = (s"${retCRUDMetrics.tgtKeyColNames}").split(",").map(_.toString)
    val keyColNames = retCRUDMetrics.tgtKeyColNames.split(",").map(_.toString) // NOSONAR

    try {
      var dfAllRecsToBeProcessed: DataFrame = null // NOSONAR
      var errRecsLogFileName: String = null
      var successRecsLogFileName: String = null
      if ((cdcActionMask & Constants.CDCActionMaskInsert) == Constants.CDCActionMaskInsert) {
        errRecsLogFileName = retCRUDMetrics.insertFailedRecsLogFileName
        successRecsLogFileName = retCRUDMetrics.insertSuccessRecsLogFileName
      } else if ((cdcActionMask & Constants.CDCActionMaskUpdate) == Constants.CDCActionMaskUpdate) {
        errRecsLogFileName = retCRUDMetrics.updateFailedRecsLogFileName
        successRecsLogFileName = retCRUDMetrics.updateSuccessRecsLogFileName
      } else if ((cdcActionMask & Constants.CDCActionMaskLogicalDelete) == Constants.CDCActionMaskLogicalDelete
        || (cdcActionMask & Constants.CDCActionMaskPhysicalDelete) == Constants.CDCActionMaskPhysicalDelete) {
        errRecsLogFileName = retCRUDMetrics.deleteFailedRecsLogFileName
        successRecsLogFileName = retCRUDMetrics.deleteSuccessRecsLogFileName
      } else {
        throw new Exception(s"getCDCProcessedRecordStatus: Invalid CRUD action mast passed($cdcActionMask)")
      }

      //If error CSV file is present, then create a dataframe with only keys
      if (errRecsLogFileName != null && errRecsLogFileName.nonEmpty) {
        val file = new File(errRecsLogFileName)
        dfCrudErrorRecsKeys = sparkSession.read.format("csv").option("header", "true")
          .load(file.toString).select(errColNames.map(name => col(name)): _*)
          .withColumn("IS_PROCESSED", lit(errorFlag))
      }
      if (successRecsLogFileName != null && successRecsLogFileName.nonEmpty) {
        val file = new File(successRecsLogFileName)
        dfSuccessRecsKeys = sparkSession.read.format("csv").option("header", "true")
          .load(file.toString)
          .select(successColNames.map(name => col(name)): _*)
          .withColumn("ERROR_DESCRIPTION", lit(""))
          .withColumn("IS_PROCESSED", lit(successFlag))
        dfSuccessRecsKeys.show
      }

      if (dfSuccessRecsKeys != null && dfCrudErrorRecsKeys != null)
        dfToBeProcessedRecWithStatus = dfSuccessRecsKeys union dfCrudErrorRecsKeys
      else if (dfSuccessRecsKeys != null)
        dfToBeProcessedRecWithStatus = dfSuccessRecsKeys
      else if (dfCrudErrorRecsKeys != null)
        dfToBeProcessedRecWithStatus = dfCrudErrorRecsKeys

      if (dfToBeProcessedRecWithStatus != null) {
        if (log.isInfoEnabled()) {
          log.info(s"getCDCProcessedRecordStatus: All to be processed record keys with status--Num of partitions->${dfToBeProcessedRecWithStatus.rdd.getNumPartitions}")
          dfToBeProcessedRecWithStatus.show()
        }
        dfToBeProcessedRecWithStatus = dfToBeProcessedRecWithStatus.coalesce(1)
        log.info(s"getCDCProcessedRecordStatus: After coalesce->${dfToBeProcessedRecWithStatus.rdd.getNumPartitions}")
      }
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
      }
    } finally {
      if (dfCrudErrorRecsKeys != null) dfCrudErrorRecsKeys.unpersist()
      if (dfSuccessRecsKeys != null) dfSuccessRecsKeys.unpersist()
    }

    log.info("getCDCProcessedRecordStatus ended.")
    dfToBeProcessedRecWithStatus
  }

  @throws(classOf[Exception])
  def truncateTable(dbConArg: Any, tableName: String): Unit = {
    var dbCon: Connection = null
    try {
      log.info("truncateTable: Started...")
      dbCon = dbConArg match {
        case _: Connection => dbConArg.asInstanceOf[Connection]
        case _: Properties => {
          val conProperties = dbConArg.asInstanceOf[Properties]
          DriverManager.getConnection(conProperties.getProperty(PropertyNames.EndPoint), conProperties)
        }
        case _: String => {
          val conProperties = getDataSourceDetails(AppProperties.SparkSession, dbConArg.asInstanceOf[String])
          DriverManager.getConnection(conProperties.getProperty(PropertyNames.EndPoint), conProperties)
        }
        case _ => throw new Exception(
          s"""truncateTable: Invalid source connection type provided ${dbConArg.getClass.getSimpleName}.
             | Expected java.sql.connection or Properties""".stripMargin)
      }
      if (tableName != null && dbCon != null) {
        val stmt = dbCon.createStatement
        stmt.executeUpdate(f"TRUNCATE TABLE $tableName IMMEDIATE")
        dbCon.commit()
        val tmpSql = s"CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE $tableName ON ALL COLUMNS WITH DISTRIBUTION ON ALL COLUMNS AND SAMPLED DETAILED INDEXES ALL')"
        log.info(tmpSql)
        stmt.executeUpdate(tmpSql)
        dbCon.commit()
        stmt.close()
        log.info("truncateTable: Ended")
      }
    } finally {
      log.info("truncateTable: Cleaning up...")
    }
  }

  def getDBConnectionHelper(dbProperties: Properties): Connection = {
    var dbCon: Connection = null
    var noOfAttempts: Int = 0
    var bException: Boolean = false

    do {
      try {
        bException = false
        dbCon = DriverManager.getConnection(dbProperties.getProperty(PropertyNames.EndPoint), dbProperties)
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          bException = true
          noOfAttempts += 1
          //Add sleep for a min here
        }
      }
    } while (noOfAttempts < 3 && bException)
    dbCon

  }

  //To Depricate
  def getCDCProcessedRecKeysWithStatus(sparkSession: SparkSession, retCRUDMetrics: CRUDMetrics, successFlag: Any,
                                       errorFlag: Any, cdcActionMask: Int): DataFrame = {
    log.info("getCDCProcessedRecordStatus")

    var bException: Boolean = false
    var dfCrudErrorRecsKeys: DataFrame = null
    var dfSuccessRecsKeys: DataFrame = null
    var dfToBeProcessedRecWithStatus: DataFrame = null
    val colNames = (s"${retCRUDMetrics.tgtKeyColNames},${Constants.CDCErrDescColName}").split(",").map(_.toString)
    val keyColNames = retCRUDMetrics.tgtKeyColNames.split(",").map(_.toString)

    try {
      var dfAllRecsToBeProcessed: DataFrame = null
      var errRecsLogFileName: String = null
      if ((cdcActionMask & Constants.CDCActionMaskInsert) == Constants.CDCActionMaskInsert) {
        dfAllRecsToBeProcessed = retCRUDMetrics.dfRecsToBeAdded
        errRecsLogFileName = retCRUDMetrics.insertFailedRecsLogFileName
      } else if ((cdcActionMask & Constants.CDCActionMaskUpdate) == Constants.CDCActionMaskUpdate) {
        dfAllRecsToBeProcessed = retCRUDMetrics.dfRecsToBeUpdated
        errRecsLogFileName = retCRUDMetrics.updateFailedRecsLogFileName
      } else if ((cdcActionMask & Constants.CDCActionMaskLogicalDelete) == Constants.CDCActionMaskLogicalDelete) {
        dfAllRecsToBeProcessed = retCRUDMetrics.dfRecsToBeDeleted
        errRecsLogFileName = retCRUDMetrics.deleteFailedRecsLogFileName
      } else if ((cdcActionMask & Constants.CDCActionMaskPhysicalDelete) == Constants.CDCActionMaskPhysicalDelete) {
        dfAllRecsToBeProcessed = retCRUDMetrics.dfRecsToBeDeleted
        errRecsLogFileName = retCRUDMetrics.deleteFailedRecsLogFileName
      } else {
        throw new Exception(s"getCDCProcessedRecordStatus: Invalid CRUD action mast passed($cdcActionMask)")
      }

      if (log.isInfoEnabled()) {
        log.info(s"getCDCProcessedRecordStatus: All keys from CDC method")
        dfAllRecsToBeProcessed.show()
        log.info(s"getCDCProcessedRecordStatus: Error record file name name -> $errRecsLogFileName")
      }

      //If error CSV file is present, then create a dataframe with only keys
      if (errRecsLogFileName != null && errRecsLogFileName.nonEmpty) {
        val file = new File(errRecsLogFileName)
        dfCrudErrorRecsKeys = sparkSession.read.format("csv").option("header", "true")
          .load(file.toString).select(colNames.map(name => col(name)): _*)
        dfSuccessRecsKeys = (dfAllRecsToBeProcessed.join(dfCrudErrorRecsKeys, keyColNames, "left_anti"))
          .withColumn("IS_PROCESSED", lit(successFlag))
        dfToBeProcessedRecWithStatus = dfSuccessRecsKeys union (dfCrudErrorRecsKeys.withColumn("IS_PROCESSED", lit(errorFlag)))
        if (log.isInfoEnabled()) {
          log.info(s"getCDCProcessedRecordStatus: Error record keys from csv file")
          dfCrudErrorRecsKeys.show()
        }
      } else {
        dfToBeProcessedRecWithStatus = dfAllRecsToBeProcessed.withColumn("IS_PROCESSED", lit(successFlag))
      }

      if (log.isInfoEnabled()) {
        log.info(s"getCDCProcessedRecordStatus: All to be processed record keys with status--Num of partitions->${dfToBeProcessedRecWithStatus.rdd.getNumPartitions}")
        dfToBeProcessedRecWithStatus.show()
      }
      dfToBeProcessedRecWithStatus = dfToBeProcessedRecWithStatus.coalesce(1)
      log.info(s"getCDCProcessedRecordStatus: After coalesce->${dfToBeProcessedRecWithStatus.rdd.getNumPartitions}")
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
      }
    } finally {
      if (dfCrudErrorRecsKeys != null) dfCrudErrorRecsKeys.unpersist()
      if (dfSuccessRecsKeys != null) dfSuccessRecsKeys.unpersist()
    }

    log.info("getCDCProcessedRecordStatus ended.")
    dfToBeProcessedRecWithStatus
  }

}
