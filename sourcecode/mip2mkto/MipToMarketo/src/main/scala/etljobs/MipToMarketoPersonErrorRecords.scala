package etljobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods.parse

import java.sql.{Connection, DriverManager, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties
import scala.sys.exit


object MipToMarketoPersonErrorRecords extends ETLFrameWork {

  //Initialization of variables
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var tgtTableName: String = null
  var apiSource: String = null
  var dbSource: String = null
  var bException: Boolean = false
  var errString: String = null
  var jobSpecific2: String = null
  var jobSpecific1: String = null
  var contactInteraction: Long = 0
  var clientInterest: Long = 0
  var digitalInteraction: Long = 0
  var eventInteraction: Long = 0

  override def getPropertiesFromJson(json: String): Properties = super.getPropertiesFromJson(json)
  var resultCount : Long = 0

  //Gets marketo token to perform post
  def getMarketoToken: String = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val clientID = apiConProps.getProperty(PropertyNames.ClientID)
    val clientSecret = apiConProps.getProperty(PropertyNames.ClientSecret)
    val identityEndpoint = apiConProps.getProperty(PropertyNames.EndPoint)
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    val httpPostToken = new HttpPost(s"$identityEndpoint&client_id=$clientID&client_secret=$clientSecret")
    val securityToken = httpClient.execute(httpPostToken, new BasicResponseHandler())

    val jValue = parse(securityToken)
    val value1 = (jValue \ "access_token").extract[String]

    value1
  }

  // API CALL IN TO GET RESULTS  - THIRD API
  def performGet(emailMediaId: String, accessToken: String): String = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val activityEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_2)
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    val httpGetToken = new HttpGet(s"$activityEndpoint?access_token=$accessToken&filterType=Email_Media_ID&filterValues=$emailMediaId")
    log.info(s"$activityEndpoint?access_token=$accessToken&filterType=Email_Media_ID&filterValues=$emailMediaId")
    val response = httpClient.execute(httpGetToken, new BasicResponseHandler())

    response
  }

  override def getDataSourceDetails(spark: SparkSession, dataSourceCode: String): Properties = super.getDataSourceDetails(spark, dataSourceCode)

  def main(args: Array[String]): Unit = {
    //Args to the job
    tgtTableName = args(args.indexOf("--tgtTable") + 1)
    apiSource = args(args.indexOf("--apiSource") + 1)
    dbSource = args(args.indexOf("--dbSource") + 1)
    contactInteraction = args(args.indexOf("--contactInteraction") + 1).toLong
    clientInterest = args(args.indexOf("--clientInterest") + 1).toLong
    digitalInteraction = args(args.indexOf("--digitalInteraction") + 1).toLong
    eventInteraction = args(args.indexOf("--eventInteraction") + 1).toLong

    log.info("Initialization started")
    this.initializeFramework(args)
    log.info("Initialization completed.")
    log.info(s"Starting ETL Job => $jobClassName....")

    val jobSequence = s"$jobClassName"
    val lastRunTimestamp = getMaxRecordTimestampTest(jobSequence)
    println(lastRunTimestamp)
    var dbCon:Connection = null

    DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobStarted, "GOOD LUCK", null, null)
    log.info("ETL logic goes here...")

    try {
      val sqlData =
        s"""(SELECT PER.EMAIL_MEDIA_ID, PER.MIP_SEQ_ID FROM MAP_MKTO.MCT_MKTO_PERSON PER
           |INNER JOIN MAP_MKTO.MCT_MKTO_CUSTOM_ACTIVITY CA ON PER.MIP_SEQ_ID = CA.MIP_SEQ_ID
           |WHERE PER.STATUS_CODE = 'E'
           |AND (
           |(PER.CREATE_TS >= CURRENT_TIMESTAMP - 30 DAYS AND CA.ACTIVITY_TYPE_ID = $contactInteraction)
           |OR (PER.CREATE_TS >= CURRENT_TIMESTAMP - 7 DAYS AND CA.ACTIVITY_TYPE_ID IN ($eventInteraction, $digitalInteraction, $clientInterest))
           |))""".stripMargin
      print(sqlData)

      val conProp1: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
      val dfResult1 = AppProperties.SparkSession.read.jdbc(conProp1.getProperty(PropertyNames.EndPoint), sqlData, conProp1)
      dfResult1.show(false)

      resultCount = dfResult1.count()
      log.info("Count of Records to be Processed: " + resultCount)

      var dfResultAggDF = dfResult1.agg(collect_set("EMAIL_MEDIA_ID"))
      log.info("EMAIL_MEDIA_ID's: " + dfResultAggDF)
      dfResultAggDF.show(false)

      val dbConnectionInfo = conProp1.getProperty(PropertyNames.EndPoint)
      dbCon = DriverManager.getConnection(dbConnectionInfo, conProp1)

      if(resultCount > 0) {

        log.info("Processing error records")

        //Variable Storing Email_Media_Ids
        var emailMediaIds = dfResultAggDF.collect().mkString(",")
        log.info(emailMediaIds)
        emailMediaIds = emailMediaIds.substring(14,emailMediaIds.length - 2).replace(" ","")
        log.info(emailMediaIds)

        log.info("API 1 STARTED")
        var accessToken = getMarketoToken
        log.info("API 1 COMPLETED")

        //CALLING 2rd API - PerformGetToken
        log.info("API 2 STARTED")
        var responses: String = performGet(emailMediaIds, accessToken)
        log.info("API 2 ENDED")
        log.info(responses)

        val spark = AppProperties.SparkSession
        //SCHEMA DECLARATION BEGINS
        import spark.implicits._
        //READING API DATA USING JSON SCHEMA
        log.info("DATAFRAME TRANSFORMATION STARTED")

        val df = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(responses)).toDS())
        log.info("RESPONSES DATAFRAME")
        df.show()

        //Traverse the result field from the JSON
        if (responses.contains("Email_Media_ID"))
          {
            var newDF = df.select(explode(col("result")).as("result"))
              .select("result.*")
            newDF.show()

            val dfResult2 = dfResult1.select("EMAIL_MEDIA_ID", "MIP_SEQ_ID").distinct()
            newDF = dfResult2.join(newDF,dfResult2("EMAIL_MEDIA_ID") === newDF("Email_Media_ID"),"left")
              .drop(newDF("Email_Media_ID"))
            newDF.show()

            var newDFAgg = newDF.groupBy("MIP_SEQ_ID").agg(max("id").alias("Lead_ID"), count("id").alias("Id_Count"))
            newDFAgg = newDFAgg.withColumn("Status_Code",expr("CASE WHEN Id_Count = 0 THEN 'U' WHEN Id_Count = 1 THEN 'P' ELSE 'E' END"))
            newDFAgg.show()

            updateErrorRecordsStatus(newDFAgg,dbCon,tgtTableName)
          }
        else {
          log.info("Status_Code for the Records To Be Updated as 'U'.")
          updateErrorRecordsStatus2Unprocessed(dfResult1,dbCon,tgtTableName)
        }
      }
      else{
        log.info("no records present to process")
      }
    }
    catch
    {
      case e: Throwable =>
        e.printStackTrace()
        errString = "Program Failed Due To: " + e.getMessage + " - " + e.getCause + "-" + e.printStackTrace() + "."
        log.error(errString)

        jobSpecific1 = "Total Count of Records Failed to Process = " + resultCount.toString
        bException = true
    }
    finally
    {
      jobSpecific2 = "Total Count of Records Processed = " + resultCount.toString
      if (bException) { // Job failed
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobFailed, errString, jobSpecific1, null)
        exit(1)
      } else {
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobSucceeded, "Job completed without any error", null, jobSpecific2)
      }
      log.info("Closing db connections")
      dbCon.close()
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }

  def updateErrorRecordsStatus(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {
    val dfUpd = dataFrame.where("Status_Code in ('U','P')").select("Status_Code","Lead_ID","MIP_SEQ_ID").toDF()
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = ?, ERROR_CODE = NULL, ERROR_DESC = NULL, MKTO_LEAD_ID = ? WHERE MIP_SEQ_ID= ?""",
      dfUpd.columns,
      Array(0,1,2),
      null,
      true,
      updateTableName,
      "UPDATE")
  }

  def updateErrorRecordsStatus2Unprocessed(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {
    val dfUpd = dataFrame.select("MIP_SEQ_ID").distinct().toDF()
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = 'U', ERROR_CODE = NULL, ERROR_DESC = NULL, MKTO_LEAD_ID = NULL WHERE MIP_SEQ_ID= ?""",
      dfUpd.columns,
      Array(0),
      null,
      true,
      updateTableName,
      "UPDATE")
  }

  @throws(classOf[Exception])
  def getMaxRecordTimestampTest(jobSeqCode: String): Timestamp = {
    var dfResult: DataFrame = null
    var maxTs: Timestamp = null
    try {
      val jobSeqHistTable: String = AppProperties.JobHistoryLogTable
      dfResult = AppProperties.SparkSession.read
        .option("isolationLevel", Constants.DBIsolationUncommittedRead)
        .jdbc(AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint),
          s""" (SELECT
                      JOB_SK JOB_SK, JOB_END_TIME AS MAX_TIMESTAMP
                    FROM $jobSeqHistTable
                    WHERE
                      JOB_SEQUENCE = '$jobSeqCode' AND JOB_STATUS = '${Constants.JobStatusSucceeded}'
                    ORDER BY JOB_SK DESC
                    FETCH FIRST ROW ONLY) AS RESULT_TABLE""",
          AppProperties.CommonDBConProperties)
      if (log.isDebugEnabled || log.isInfoEnabled()) dfResult.show(false)
      if (dfResult.count() > 0) {
        val firstRow = dfResult.collect().head
        maxTs = firstRow.getTimestamp(1)
      }
      else
      {
        maxTs = Timestamp.valueOf(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").format(LocalDateTime.now(ZoneOffset.UTC)))
      }
    } finally {
      if (dfResult != null) dfResult.unpersist()
    }
    maxTs
  }
}