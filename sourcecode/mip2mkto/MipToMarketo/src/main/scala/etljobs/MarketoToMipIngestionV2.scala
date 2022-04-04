package etljobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.commons.lang.time.DateUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.sql.functions.{to_timestamp, _}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructType, TimestampType}
import org.json4s.jackson.JsonMethods.parse

import java.sql.{Connection, DriverManager, Statement}
import java.text.SimpleDateFormat
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MarketoToMipIngestionV2 extends ETLFrameWork {

  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var tgtTableName: String = null
  var apiSource: String = null
  var dbTgtSource: String = null
  var configSource: String = null
  var firstRunTs: String = null
  var maxRecordTimestamp: String = null
  var errstr: String = null
  var recordsProcessed: Long = 0
  var dupCount: Long = 0
  var batchSize = 1000
  var apiBatchRowCount : Long = 0
  var apiRowCount: Long =0
  var jobSpecific2: String = null
  var rejectMessage: String = ""
  var softErrorMessage: String = ""
  var newDFUnmappedColumn: DataFrame = null
  var emailActivityFinalDF: DataFrame = null
  var emailActivityTempDF: DataFrame = null
  var mapDfToDBColumns: mutable.Map[String, String] = mutable.Map[String, String]()
  var bException: Boolean = false
  case class Attributes(name: String, value: String)


  def main(args: Array[String]): Unit = {
    args.foreach(println(_))
    tgtTableName = args(args.indexOf("--tgtTable") + 1)
    apiSource = args(args.indexOf("--apiSource") + 1)
    dbTgtSource = args(args.indexOf("--dbTgtSource") + 1)
    configSource = args(args.indexOf("--configSource") + 1)
    firstRunTs = args(args.indexOf("--firstRunTs") + 1)

    try {
      log.info("INITIALIZATION STARTED")
      this.initializeFramework(args)
      log.info("INITIALIZATION COMPLETED.")
      log.info(s"STARTING ETL JOB => $jobClassName....")
      log.info("ETL_JOB_HIST TABLE INSERT STARTED")
      DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants
        .JobStarted, "GOOD LUCK", null, null)
      log.info("ETL_JOB_HIST TABLE INSERT ENDED")
      runJobSequence(tgtTableName)
      log.info(s"COMPLETED JOB => $jobClassName.")
    }
    catch {
      case e: Throwable =>
        e.printStackTrace()
        errstr = "Program Failed Due To: " + e.getMessage + " - " + e.getCause + "-" + e.printStackTrace() + ". " +
          softErrorMessage + ". " + rejectMessage
        jobSpecific2 = "Total Count of Records Processed = " + recordsProcessed.toString()
        log.error(errstr)
        bException = true
    }
    finally {
      val jobSpecific1 = getMaxActivityDate
      if (bException) { // Job failed
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants
          .JobFailed, errstr, jobSpecific1, jobSpecific2)
        System.exit(1)
      }
      else {
        var messageString: String = null
        if(softErrorMessage != "" || dupCount > 0){
          messageString = "Job Completed With Warnings. " + softErrorMessage + " " + rejectMessage
        }
        else{
          messageString = "Job Completed Successfully Without Any Errors. " + rejectMessage
        }
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants
          .JobSucceeded, messageString, jobSpecific1, jobSpecific2)
      }
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"EXITING JOB => $jobClassName...")
      System.exit(0)
    }
  }


  //  RUNJOBSEQUENCE METHOD = API CALLING, DATAFRAME INGESTION & LOOPING, AND INGESTING DATAFRAME TO DB2 TABLE
  @throws(classOf[Exception])
  def runJobSequence(tgtTableName: String): Unit = {
    log.info("RUNJOBSEQUENCE STARTED")
    log.info("V2 Version.batchSize" + batchSize)
    // TODO: ETL Logic goes here...

    // DEFINING TIMESTAMP TO STRING & BACK TO TIMESTAMP - START
    log.info("API TIMESTAMP STARTED")
    maxRecordTimestamp = getLastRecordTimeStamp(jobClassName)
    if (maxRecordTimestamp == null || maxRecordTimestamp == "" || maxRecordTimestamp == "null") {
      maxRecordTimestamp = firstRunTs
    }
    log.info("LAST RECORDED TIMESTAMP:" + maxRecordTimestamp)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val apidate = dateFormat.parse(maxRecordTimestamp)
    val apiNewDate = DateUtils.addSeconds(apidate,1)
    val apiDateStr = dateFormat.format(apiNewDate)
    val parsedMaxRecordTimestamp = apiDateStr.substring(0, 10) + "T" + apiDateStr.substring(11, 13) +
      ":" + apiDateStr.substring(14, 16) + ":" + apiDateStr.substring(17, 21) + "-00:00"
    val utcMaxRecordTimestamp = OffsetDateTime.parse(parsedMaxRecordTimestamp).withOffsetSameInstant(ZoneOffset.UTC).toString()
    log.info("CONVERTED TO UTC-TIMESTAMP:" + utcMaxRecordTimestamp.toString)

    val apiTimestamp: String = utcMaxRecordTimestamp
    log.info("API TIMESTAMP ENDED")
    // DEFINING TIMESTAMP TO STRING & BACK TO TIMESTAMP - END

    // CALLING THE 1st API - GetMarketoToken
    log.info("API 1 STARTED")
    val mktCreds: ArrayBuffer[String] = getMarketoToken
    var accessToken = mktCreds(0)
    val userName = mktCreds(1)
    log.info("API 1 COMPLETED")

    // CALLING 2nd API - GetPaginationToken
    log.info("API 2 STARTED")
    var paginationToken = getPaginationToken(apiTimestamp, accessToken)
    log.info("API 2 ENDED")

    //CALLING 3rd API - PerformGetToken
    log.info("API 3 STARTED")
    var response = performGet(paginationToken, accessToken)
    log.info("API 3 ENDED")

    //USE EXTERNAL JSON INSTEAD OF 3rd API-3 START
    //val input_file = "/Users/ksuryadevara/Documents/failed2.json"
    //var response = scala.io.Source.fromFile(input_file).mkString
    //USE EXTERNAL JSON INSTEAD OF 3rd API END

    // COLUMN MAPPING STARTS
    log.info("COLUMN MAPPING STARTED")
    val MKTO_MIP_JSON_PROPS = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, configSource)
    val columnMapping = MKTO_MIP_JSON_PROPS.getProperty(PropertyNames.ResourceSpecific_2)

    for (curVal <- columnMapping.split(",")) {
      val arrVal = curVal.split("=")
      mapDfToDBColumns += arrVal(1) -> arrVal(0)
    }
    log.info("COLUMN MAPPING ENDED")
    // COLUMN MAPPING ENDS - NOTE: In Mapping: processed_flag(SOURCE/KEYS) = PROCESSED_FLAG(TARGET/VALUES)

    //INSERTING API RESPONSES INTO DATAFRAME LOOP - STARTED
    log.info("MARKETO TO MIP LOGIC STARTED")
    emailActivityFinalDF = AppProperties.SparkSession.createDataFrame(AppProperties.SparkSession
      .sparkContext.emptyRDD[Row], StructType(List()))
    log.info("API JSON TO DATAFRAME STARTED")

    // CHECKING THE 3rd API IF ACCESS TOKEN HAS EXPIRED OR NOT - STARTED
    if (response.contains("Access token expired") || response.contains("Access token invalid")) {
      val mktCreds2: ArrayBuffer[String] = getMarketoToken
      accessToken = mktCreds2(0)
      response = performGet(paginationToken, accessToken)
    }
    // CHECKING THE 3rd API IF ACCESS TOKEN HAS EXPIRED OR NOT - ENDED

    // PARSING THE RESULTS - START
    if (response.contains("result") || response.contains("moreResult")) {
      var jValue = parse(response)
      var moreResult = (jValue \ "moreResult").extract[Boolean]
      if (!moreResult) {
        if (response.contains("result")) {
          emailActivityFinalDF = marketoMipLogic(response, mapDfToDBColumns, userName)
          apiBatchRowCount = emailActivityFinalDF.count()
        }
        else {
          apiBatchRowCount = 0
          log.info("NO DATA AVAILABLE FOR PROCESSING")
        }
      }
      else {
        var i: Int = 0
        var j: Int = 0
        while (moreResult) {
          i = i + 1
          log.info("PAGE COUNT:" + i + " ,moreResults:" + moreResult)
          response = performGet(paginationToken, accessToken)
          jValue = parse(response)
          if (response.contains("Access token expired") || response.contains("Access token invalid")) {
            val mktCreds: ArrayBuffer[String] = getMarketoToken
            accessToken = mktCreds(0)
            response = performGet(paginationToken, accessToken)
            jValue = parse(response)
            paginationToken = (jValue \ "nextPageToken").extract[String]
            moreResult = (jValue \ "moreResult").extract[Boolean]
            if (response.contains("result")) {
              j = j + 1
              emailActivityTempDF = marketoMipLogic(response, mapDfToDBColumns, userName)
              if (j == 1) {
                emailActivityFinalDF = AppProperties.SparkSession.createDataFrame(AppProperties.SparkSession
                  .sparkContext.emptyRDD[Row], emailActivityTempDF.schema)
              }
              emailActivityFinalDF = emailActivityFinalDF.unionByName(emailActivityTempDF)
              emailActivityFinalDF.cache()
              apiBatchRowCount = emailActivityFinalDF.count()
              log.info("BATCH ROW COUNT:" + apiBatchRowCount)
              if (apiBatchRowCount >= batchSize) {
                apiRowCount = apiRowCount + apiBatchRowCount
                emailActivityInsert(emailActivityFinalDF, tgtTableName)
                emailActivityFinalDF = AppProperties.SparkSession.createDataFrame(AppProperties.SparkSession
                  .sparkContext.emptyRDD[Row], emailActivityTempDF.schema)
                apiBatchRowCount = 0
              }
            }
          }
          else {
            paginationToken = (jValue \ "nextPageToken").extract[String]
            moreResult = (jValue \ "moreResult").extract[Boolean]
            if (response.contains("result")){
              j = j + 1
              emailActivityTempDF = marketoMipLogic(response, mapDfToDBColumns, userName)
              if (j == 1) {
                emailActivityFinalDF = AppProperties.SparkSession.createDataFrame(AppProperties.SparkSession
                  .sparkContext.emptyRDD[Row], emailActivityTempDF.schema)
              }
              emailActivityFinalDF = emailActivityFinalDF.unionByName(emailActivityTempDF)
              emailActivityFinalDF.cache()
              apiBatchRowCount = emailActivityFinalDF.count()
              log.info("BATCH ROW COUNT:" + apiBatchRowCount)
              if (apiBatchRowCount >= batchSize) {
                apiRowCount = apiRowCount + apiBatchRowCount
                emailActivityInsert(emailActivityFinalDF, tgtTableName)
                emailActivityFinalDF = AppProperties.SparkSession.createDataFrame(AppProperties.SparkSession
                  .sparkContext.emptyRDD[Row], emailActivityTempDF.schema)
                apiBatchRowCount = 0
              }
            }
          }
        }
      }
    }
    else {
      apiRowCount=0
      log.info("NO DATA AVAILABLE FOR PROCESSING")
    }
    // PARSING THE RESULTS - ENDED
    log.info("API JSON TO DATAFRAME ENDED")
    //INSERTING API RESPONSES INTO DATAFRAME LOOP - ENDED

    //INGESTING DATAFRAME TO DB - STARTED
    if(apiBatchRowCount==0) {
      log.info("NO DATA TO INSERT. EXITING.")
    }
    else {
      log.info("DB2 INSERT STARTED")
      apiRowCount = apiRowCount + apiBatchRowCount
      emailActivityInsert(emailActivityFinalDF, tgtTableName)
      log.info("DB2 INSERT ENDED")
      //INGESTING DATAFRAME TO DB - ENDED
    }
    jobSpecific2 = "Total Count of Records Processed = " + recordsProcessed.toString()
    log.info("RUNJOBSEQUENCE METHOD ENDED")
    log.info("MARKETO TO MIP LOGIC COMPLETED")
  }


  // CALL IN MARKETO TOKEN - FIRST API
  def getMarketoToken: ArrayBuffer[String] = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val clientID = apiConProps.getProperty(PropertyNames.ClientID)
    val clientSecret = apiConProps.getProperty(PropertyNames.ClientSecret)
    val identityEndpoint = apiConProps.getProperty(PropertyNames.EndPoint)
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    val httpPostToken = new HttpPost(s"$identityEndpoint&client_id=$clientID&client_secret=$clientSecret")
    val securityToken = httpClient.execute(httpPostToken, new BasicResponseHandler())

    val jValue = parse(securityToken)
    var marketoCreds: ArrayBuffer[String] = ArrayBuffer[String]()
    marketoCreds += (jValue \ "access_token").extract[String]
    marketoCreds += (jValue \ "scope").extract[String]

    marketoCreds
  }


  // CALL IN PAGINATION TOKEN - SECOND API
  def getPaginationToken(sinceDateTime: String, accessToken: String): String = {
    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val paginationEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_3)
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    var httpPostToken = new HttpGet(s"$paginationEndpoint?access_token=$accessToken&sinceDatetime=$sinceDateTime")
    var response = httpClient.execute(httpPostToken, new BasicResponseHandler())
    if (response.contains("Access token expired") || response.contains("Access token invalid")) {
      val mktCreds1: ArrayBuffer[String] = getMarketoToken
      val accessToken1 = mktCreds1(0)
      httpPostToken = new HttpGet(s"$paginationEndpoint?access_token=$accessToken1&sinceDatetime=$sinceDateTime")
      response = httpClient.execute(httpPostToken, new BasicResponseHandler())
    }
    val jValue = parse(response)
    val value1 = (jValue \ "nextPageToken").extract[String]
    value1
  }


  // API CALL IN TO GET RESULTS  - THIRD API
  def performGet(paginationToken: String, accessToken: String): String = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val activityEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_2)
    val activityId = apiConProps.getProperty(PropertyNames.ResourceSpecific_4)
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    val httpGetToken = new HttpGet(
      s"$activityEndpoint?access_token=$accessToken&activityTypeIds=$activityId&nextPageToken=$paginationToken")
    val response = httpClient.execute(httpGetToken, new BasicResponseHandler())

    response
  }


  // METHOD TO CREATE DATAFRAME SCHEMA, MAPPING, AND SOFT ERRORS
  def marketoMipLogic(response: String, mapDfToDBColumns: mutable.Map[String, String], userName: String): DataFrame = {

    val spark = AppProperties.SparkSession
    var emailActivityDF: DataFrame = null

    //SCHEMA DECLARATION BEGINS
    import spark.implicits._
    log.info("ETL LOGIC STARTS HERE...")

    // CREATING DATAFRAME FROM RESPONSE, EXPANDING THE ATTRIBUTES & PIVOTING TO CREATE NEW COLUMNS - START
    //CREATING DATA FRAME USING JSON SCHEMA FROM DB2 - START
    log.info("DATAFRAME SCHEMA CREATION FROM ETL_DATA_SOURCES TABLE STARTED")
    val getMarketoMipJsonProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, configSource)
    var dataFrameSchema = StructType.fromDDL(getMarketoMipJsonProps.getProperty(PropertyNames.ResourceSpecific_1))
    log.info("DATAFRAME SCHEMA CREATION FROM ETL_DATA_SOURCES TABLE ENDED")
    //CREATING DATA FRAME USING JSON SCHEMA FROM DB2 - END

    //READING API DATA USING JSON SCHEMA
    log.info("DATAFRAME TRANSFORMATION STARTED")
    val df = AppProperties.SparkSession.read
      .schema(dataFrameSchema)
      .json(AppProperties.SparkSession.sparkContext.parallelize(Seq(response)).toDS())

    //Traverse the result field from the JSON
    var newDF = df.select(explode(col("result")).as("result"))
      .select("result.*")
    var colList = newDF.columns.toList

    //SAVING DATAFRAME RESULTS TO SPARKSQL TABLE - START
    newDF.createOrReplaceTempView("newDF")
    val columnsToUse:String = "id,marketoGUID,leadId,activityDate,activityTypeId,campaignId,primaryAttributeValueId," +
      "primaryAttributeValue,attributes"

    val selectSql1 = s"""SELECT $columnsToUse FROM newDF""".stripMargin
    newDF = spark.sql(selectSql1)
    newDF = newDF.withColumn("attributes",explode(col("attributes").as[Attributes]))
    newDF.createOrReplaceTempView("selectDF")

    val selectSql2 = s"""SELECT $columnsToUse,attributes.name as Name, attributes.value as Value FROM selectDF""".stripMargin
    newDF = spark.sql(selectSql2).drop("attributes")

    var tempDF = newDF
    tempDF = tempDF.drop("Name","Value")

    val groupCols:List[String] = tempDF.columns.toList
    newDF = newDF.groupBy(groupCols.head,groupCols.tail: _*).pivot("Name").agg(first("Value"))
    colList = newDF.columns.toList
    //SAVING DATAFRAME RESULTS TO SPARKSQL TABLE - END
    //CREATING DATAFRAME FROM RESPONSE, EXPANDING THE ATTRIBUTES & PIVOTING TO CREATE NEW COLUMNS - ENDS

    // REMOVING SPACES IN THE COLUMN NAMES
    for (i <- colList) {
      val j = i.replace(" ", "")
      newDF = newDF.withColumnRenamed(i, j)
      newDF = newDF.withColumn(j, rtrim(col(j)))
    }

    // ADDING THE COLUMNS TO DATAFRAME WITH DEFAULT VALUE AS NULL -  START
    colList = newDF.columns.toList
    val diffCols = mapDfToDBColumns.values.toList diff colList

    for (i <- diffCols) {
      newDF = newDF.withColumn(i, lit(null).cast("string"))
    }
    // ADDING THE COLUMNS TO DATAFRAME WITH DEFAULT VALUE AS NULL -  END

    // UPDATING AUDIT COLUMNS TO DATAFRAME - START
    newDF = newDF.withColumn("activityDate", to_timestamp(col("activityDate")))
      .withColumn("create_ts", to_utc_timestamp(current_timestamp, "EST"))
      .withColumn("update_ts", to_utc_timestamp(current_timestamp, "EST"))
      .withColumn("create_user", lit(userName))
      .withColumn("processed_flag", lit("N"))
    log.info("DATAFRAME TRANSFORMATION ENDED")
    // UPDATING AUDIT COLUMNS TO DATAFRAME - END

    // HANDLING SOFT ERROR FOR NEW COLUMNS - START
    val finalCols = newDF.columns
    val compareCols = mapDfToDBColumns.values.toArray

    for (i <- finalCols) {
      if (!compareCols.contains(i)) {
        newDF.createOrReplaceTempView("newDF")
        newDFUnmappedColumn = spark.sql("SELECT id, activityTypeID, " +i+ " FROM newDF WHERE " + i + " IS NOT NULL")

        log.info("NEW COLUMN/S " + i + " FOUND IN API RESPONSE. " +
          "MANUALLY ADD DATA TYPE & MAPPING TO RESOURCE_SPECIFIC 1 & 2 COLUMNS IN ETL_DATA_SOURCES TABLE")
        softErrorMessage = softErrorMessage + "New Column/s " + i + " found in API response. " +
          "Manually Add Data Type & Mapping To Resource_Specific Columns In ETL_DATA_SOURCES Table"

        //  To Successfully perform Unions of Data Frames, we Add any new columns in the subsequent API calls.
        emailActivityFinalDF = emailActivityFinalDF.withColumn(i,lit(""))
        newDF = newDF.withColumn(i, col(i).substr(0, 500))
        addUnmappedColumns(i)
        mapDfToDBColumns += i -> i
      }
    }
    // HANDLING SOFT ERROR FOR NEW COLUMNS - END

    // MAPPING API COLUMN NAMES TO DB COLUMN NAMES - START
    val newDf_Rename = mapDfToDBColumns.foldLeft(newDF){
      case(data,(newname,oldname)) => data.withColumnRenamed(oldname,newname)
    }
    emailActivityDF = newDf_Rename

    // APPLYING DATA TYPE CONVERSIONS - START
    log.info("DATA TYPE CONVERSION STARTED")
    emailActivityDF = emailActivityDF
      .withColumn("id", col("id").cast(IntegerType))
      .withColumn("marketo_guid", col("marketo_guid").substr(0,64))
      .withColumn("lead_id", col("lead_id").cast(IntegerType))
      .withColumn("activity_date", col("activity_date").cast(TimestampType))
      .withColumn("activity_type_id", col("activity_type_id").cast(IntegerType))
      .withColumn("campaign_id", col("campaign_id").cast(IntegerType))
      .withColumn("primary_attribute_value_id", col("primary_attribute_value_id").cast(IntegerType))
      .withColumn("primary_attribute_value", regexp_replace(col("primary_attribute_value"),
        "[^\\x00-\\x7F]", "").substr(0,3000))
      .withColumn("client_ip_address", regexp_replace(col("client_ip_address")
        ,"[^\\x00-\\x7F]", "").substr(0,25))
      .withColumn("query_parameters", regexp_replace(col("query_parameters"),
        "[^\\x00-\\x7F]", "").substr(0,500))
      .withColumn("referrer_url", regexp_replace(col("referrer_url"),
        "[^\\x00-\\x7F]", "").substr(0,255))
      .withColumn("user_agent", regexp_replace(regexp_replace(col("user_agent"),
        "[^\\x00-\\x7F]", ""),"\\\\", "").substr(0,300))
      .withColumn("campaign", regexp_replace(col("campaign"),
        "[^\\x00-\\x7F]", "").substr(0,255))
      .withColumn("webpage_id", col("webpage_id").cast(IntegerType))
      .withColumn("link_id", col("link_id").cast(IntegerType))
      .withColumn("campaign_run_id", col("campaign_run_id").cast(IntegerType))
      .withColumn("choice_number", col("choice_number").cast(IntegerType))
      .withColumn("has_predictive", col("has_predictive").cast(BooleanType))
      .withColumn("step_id", col("step_id").cast(IntegerType))
      .withColumn("test_variant", col("test_variant").cast(IntegerType))
      .withColumn("mailing_id", regexp_replace(col("mailing_id"),
        "[^\\x00-\\x7F]", "").substr(0,64))
      .withColumn("category", regexp_replace(col("category"),
        "[^\\x00-\\x7F]", "").substr(0,64))
      .withColumn("details", regexp_replace(col("details"),
        "[^\\x00-\\x7F]", "").substr(0,255))
      .withColumn("email", regexp_replace(col("email"),
        "[^\\x00-\\x7F]", "").substr(0,64))
      .withColumn("sub_category", regexp_replace(col("sub_category"),
        "[^\\x00-\\x7F]", "").substr(0,64))
      .withColumn("form_fields", regexp_replace(col("form_fields"),
        "[^\\x00-\\x7F]", "").substr(0,2000))
      .withColumn("web_form_id", col("web_form_id").cast(IntegerType))
      .withColumn("device", regexp_replace(col("device"),
        "[^\\x00-\\x7F]", "").substr(0,100))
      .withColumn("is_mobile_device", col("is_mobile_device").cast(BooleanType))
      .withColumn("platform", col("platform").substr(0,64))
      .withColumn("link", regexp_replace(col("link"),
        "[^\\x00-\\x7F]", "").substr(0,3000))
      .withColumn("sent_to_list", regexp_replace(col("sent_to_list"),
        "[^\\x00-\\x7F]", "").substr(0,3000))
      .withColumn("sent_to_owner", regexp_replace(col("sent_to_owner"),
        "[^\\x00-\\x7F]", "").substr(0,25))
      .withColumn("sent_to_smart_list", regexp_replace(col("sent_to_smart_list"),
        "[^\\x00-\\x7F]", "").substr(0,300))
      .withColumn("processed_flag", regexp_replace(col("processed_flag"),
        "[^\\x00-\\x7F]", "").substr(0,1))
      .withColumn("is_predictive", col("is_predictive").cast(BooleanType))
    log.info("DATA TYPE CONVERSION ENDED")
    emailActivityDF.show(false)
    // APPLYING DATA TYPE CONVERSIONS - END
    emailActivityDF
    // MAPPING API COLUMN NAMES TO DB COLUMN NAMES - END
  }


  //INGESTING THE DATA COMPILED FROM THE API DATAFRAME TO DB2 & COUNTING NUMBER OF RECORDS INGESTED
  def emailActivityInsert(newDF: DataFrame, tgtTableName: String): Unit = {
    // DATABASE CONNECTION - START
    log.info("DB2 INSERT STARTED")
    println("WRITING TO DB2")
    // DATABASE CONNECTION PROPERTIES - START
    val connectionProperties: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession,dbTgtSource)
    connectionProperties.put("driver","com.ibm.db2.jcc.DB2Driver")
    connectionProperties.put("sslConnection","true")
    // DATABASE CONNECTION PROPERTIES - END

    // READING THE MIP DATABASE - START
    val mipEmailActivityDF = AppProperties.SparkSession.read
      .jdbc(connectionProperties.getProperty(PropertyNames.EndPoint),tgtTableName,connectionProperties)
    // READING THE MIP DATABASE - END

    // LISTING THE DUPLICATE RECORDS FOUND IN THE API CALL - START
    log.info("CHECKING FOR DUPLICATES")
    val dupDF = newDF.join(mipEmailActivityDF,mipEmailActivityDF("ID")===newDF("id"),"inner")
      .select(newDF("*"))
    dupDF.select(dupDF("id")).collect.foreach(println)
    dupCount = dupCount + dupDF.count()
    rejectMessage = "Total Count of Duplicate Rows Found & Rejected = " + dupCount
    log.info("TOTAL COUNT OF DUPLICATE ROWS FOUND & REJECTED = " + dupCount)
    // LISTING THE DUPLICATE RECORDS FOUND IN THE API CALL - END

    // WRITING THE DISTINCT RECORDS TO MIP DATABASE - START
    val newDFFinal = newDF.join(mipEmailActivityDF,mipEmailActivityDF("ID")===newDF("id"),"left_anti")
      .select(newDF("*"))
    newDFFinal.write.mode("append")
      .option("batchSize", 1000)
      .jdbc(connectionProperties.getProperty(PropertyNames.EndPoint),tgtTableName,connectionProperties)
    // WRITING THE DISTINCT RECORDS TO MIP DATABASE - END

    //COUNTING TOTAL NUMBER OF RECORDS INSERTED - START
    recordsProcessed = apiRowCount - dupCount
    log.info("TOTAL RECORDS INSERTED, TOTAL NUMBER OF BATCH RECORDS :" + recordsProcessed.toString() + "," + apiBatchRowCount.toString())
    //COUNTING TOTAL NUMBER OF RECORDS INSERTED - END
    //DATABASE CONNECTION - END
  }


  // METHOD TO EXTRACT MAX-TIMESTAMP FROM ACTIVITY_DATE COLUMN OF MKTO_EMAIL_ACTIVITY TABLE TO BE
  // UPDATED INTO ETL_JOB_HISTORY TABLE
  @throws(classOf[Exception])
  def getMaxActivityDate(): String = {
    // "yyyy-MM-dd HH:mm:ss.SSS"
    val conProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbTgtSource)
    var dfResult: DataFrame = null
    var maxTs:String = null
    try {
      dfResult = AppProperties.SparkSession.read
        .option("isolationLevel", Constants.DBIsolationUncommittedRead)
        .jdbc(conProp.getProperty(PropertyNames.EndPoint),
          s""" (SELECT
                      MAX(ACTIVITY_DATE) - 5 MINUTES as activity_date
                       FROM $tgtTableName)
                       AS RESULT_TABLE""",
          conProp)
      if (log.isDebugEnabled || log.isInfoEnabled())
      //        dfResult.show()
        if (dfResult.count() > 0) {
          val firstRow = dfResult.collect().head
          try {
            if (firstRow.getTimestamp(0) != null) {
              maxTs = firstRow.getTimestamp(0).toString
            }
          }
        }
        else maxTs = null
    } finally {
      if (dfResult != null) dfResult.unpersist()
    }
    maxTs
  }


  // METHOD TO EXTRACT MAX-TIMESTAMP FROM ETL_JOB_HISTORY TABLE USED FOR CALLING API PAGINATION TOKEN
  @throws(classOf[Exception])
  def getLastRecordTimeStamp(jobSeqCode: String): String = {
    // "yyyy-MM-dd HH:mm:ss.SSS"
    var dfResult: DataFrame = null
    var maxTs:String = null
    try {
      val jobSeqHistTable: String = AppProperties.JobHistoryLogTable
      dfResult = AppProperties.SparkSession.read
        .option("isolationLevel", Constants.DBIsolationUncommittedRead)
        .jdbc(AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint),
          s""" (SELECT
                      JOB_SK JOB_SK, JOB_SPECIFIC_1 AS MAX_TIMESTAMP
                    FROM $jobSeqHistTable
                    WHERE
                      JOB_SEQUENCE = '$jobSeqCode'
                     AND JOB_STATUS != 'Started'
                    ORDER BY JOB_SK DESC
                    FETCH FIRST ROW ONLY) AS RESULT_TABLE""",
          AppProperties.CommonDBConProperties)
      if (log.isDebugEnabled || log.isInfoEnabled())
        dfResult.show()
      if (dfResult.count() > 0) {
        val firstRow = dfResult.collect().head
        maxTs = firstRow.getString(1)
      }
      else maxTs = null
    } finally {
      if (dfResult != null) dfResult.unpersist()
    }
    maxTs
  }


  // METHOD TO ALTER TABLE TO ADD A NEW COLUMN FOUND IN API RESPONSES
  @throws(classOf[Exception])
  def addUnmappedColumns(columnName: String) = {
    val tblNameWithoutSchema = tgtTableName.split("\\.")(1)
    val tblSchemaName = tgtTableName.split("\\.")(0)
    var dbCon: Connection = null
    var stmt: Statement = null
    val sql = s""" BEGIN
                 |IF (NOT EXISTS(
                 |SELECT 1 FROM SYSCAT.COLUMNS
                 |  WHERE UPPER(TABSCHEMA) = UPPER('$tblSchemaName')
                 |      AND UPPER(TABNAME) = UPPER('$tblNameWithoutSchema')
                 |      AND UPPER(COLNAME) = UPPER('$columnName')))
                 |THEN
                 |  EXECUTE IMMEDIATE 'ALTER TABLE $tgtTableName ADD COLUMN $columnName varchar(500)';
                 |END IF;
                 |END;""".stripMargin
    val sql1 = s""" call sysproc.admin_cmd('reorg TABLE $tgtTableName');"""
    val dbConnectionInfo: String = AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint)
    try {
      dbCon = DriverManager.getConnection(dbConnectionInfo, AppProperties.CommonDBConProperties)
      stmt = dbCon.createStatement
      stmt.executeUpdate(sql)
      stmt.executeUpdate(sql1)
      dbCon.commit()
    }
    finally {
      if (dbCon != null) dbCon.close()
    }
  }
}