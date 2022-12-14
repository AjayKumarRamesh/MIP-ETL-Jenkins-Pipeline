package etljobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods.parse
import play.api.libs.functional.syntax.toFunctionalBuilderOps
import play.api.libs.json.Reads._
import play.api.libs.json.{JsPath, Json, Reads}

import java.net.SocketTimeoutException
import java.sql.{Connection, DriverManager, Statement, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.exit


object MipToMarketoPersonMulti extends ETLFrameWork {

  //Initialization of variables
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var tgtTableName: String = null
  var apiSource: String = null
  var dbSource: String = null
  var configSource: String = null
  var bException: Boolean = false
  var minBatchSize: Int = 0
  var maxThreshold: Int = 0
  var elapsedTime: Int = 0
  var action: String = null
  var revertDF: DataFrame = null
  var partitionNo: String = null


  override def getPropertiesFromJson(json: String): Properties = super.getPropertiesFromJson(json)
  var lookupField: String = null
  var finalJoinedDFCount: Long = 0
  var count : Long = 0
  var count2 : Long = 0
  var errorCounter : String = null


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

  //Code to build payload to POST to Marketo
  def buildPayload(transformedDF: DataFrame, action: String, lookUpField: String): String = {
    val outputDf = transformedDF.select(to_json(struct(col("*"))).alias("content"))
    val testData = outputDf.rdd.map(row => row.getString(0)).collect
    val inputData: String = testData.mkString(",")
    val strNew1  = inputData.replaceAll("[\"][a-zA-Z0-9_]*[\"]:\"\"[,]?", "")
    val finalStr  = strNew1.replaceAll("[,]?}", "}")
    val payload =
      s"""{
         |    \"action\": \"$action\",
         |    \"lookupField\": \"$lookUpField\",
         |    \"input\": [ $finalStr ]
         |}
         |""".stripMargin

    payload
  }

  //Function to perform POST to Marketo
  def sendPost(payload: String): String = {

    val apiConProps:Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val leadEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
    var postResponse: String = null
    val httpPostToken = null
    val httpClient: CloseableHttpClient = HttpClients.custom().build()

    //Exception handling for duplicates in Payload
    try {
      val token = getMarketoToken
      val httpPostToken = new HttpPost(s"$leadEndpoint?access_token=$token")
      httpPostToken.addHeader("Content-type", "application/json;charset=UTF-8")
      httpPostToken.addHeader("Accept", "application/json")
      httpPostToken.setEntity(new StringEntity(payload, "UTF-8"))
      postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())

    }
    catch {
      // Case statement
      case _: SocketTimeoutException =>
        //Second Call
        postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())
    }
    postResponse
  }

  def sendPostWithToken(payload: String, token: String): String = {

    val apiConProps:Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val leadEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
    var postResponse: String = null
    val httpPostToken = null
    val httpClient: CloseableHttpClient = HttpClients.custom().build()

    //Exception handling for duplicates in Payload
    try {
      val httpPostToken = new HttpPost(s"$leadEndpoint?access_token=$token")
      httpPostToken.addHeader("Content-type", "application/json;charset=UTF-8")
      httpPostToken.addHeader("Accept", "application/json")
      httpPostToken.setEntity(new StringEntity(payload, "UTF-8"))
      postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())

    }
    catch {
      // Case statement
      case _: SocketTimeoutException =>
        //Second Call
        postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())
    }
    postResponse
  }


  override def getDataSourceDetails(spark: SparkSession, dataSourceCode: String): Properties = super.getDataSourceDetails(spark, dataSourceCode)

  def main(args: Array[String]): Unit = {
    //Args to the job
    tgtTableName = args(args.indexOf("--tgtTable") + 1)
    apiSource = args(args.indexOf("--apiSource") + 1)
    dbSource = args(args.indexOf("--dbSource") + 1)
    configSource = args(args.indexOf("--configSource") + 1)
    minBatchSize = args(args.indexOf("--minBatchSize") + 1).toInt
    maxThreshold = args(args.indexOf("--maxThreshold") + 1).toInt
    elapsedTime = args(args.indexOf("--elapsedTime") + 1).toInt
    action = args(args.indexOf("--action") + 1)
    lookupField = args(args.indexOf("--lookupField") + 1)
    partitionNo = args(args.indexOf("--partitions") + 1)


    log.info("Initialization started")
    this.initializeFramework(args)
    log.info("Initialization completed.")
    log.info(s"Starting ETL Job => $jobClassName....")

    val jobSequence = s"$jobClassName"
    val lastRunTimestamp = getMaxRecordTimestampTest(jobSequence)
    println(lastRunTimestamp)
    var mip_seq_id = Array[MipToMarketoPersonMulti.mipSeqId]() //NOSONAR
    var lead_id = Array[MipToMarketoPersonMulti.leadId]() //NOSONAR
    var dbCon:Connection = null

    var mapIds = scala.collection.mutable.Map[Long, Long]()

    DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobStarted, "GOOD LUCK", null, null)
    log.info("ETL logic goes here...")

    try {
      val sqlData =
        s"""(WITH ranked_data AS (
           |SELECT DISTINCT IDM_ID,
           |RANK() OVER(
           |ORDER BY
           |p.CREATE_TS ASC
           |) ranking,
           |EMAIL_MEDIA_ID, NEW_TO_IDM_IND, FIRST_NAME , LAST_NAME , EMAIL_ADDR , COMPANY_NAME , CTRY_CODE , IDM_COMPANY_ID , DQ_EMAIL_IND , DQ_NAME_IND , DQ_PHONE_IND, CREATE_TS, IDM_FEDGOV_IND,
           |PREF_CODE_IBM, COMPANY_PHONE_SUPR_CODE, COMPANY_EMAIL_SUPR_CODE, PREF_CODE_17YNI, PREF_CODE_17ENL, PREF_CODE_17DSR, PREF_CODE_17CPH, PREF_CODE_17BCH, PREF_CODE_17AAL, PREF_CODE_15WSC,
           |PREF_CODE_15WCP,PREF_CODE_15STT, PREF_CODE_15MFT, PREF_CODE_15ITT, PREF_CODE_15IGO, PREF_CODE_15CLV, PREF_CODE_153QH, PREF_CODE_10N00, PREF_CODE_10M00, PREF_CODE_10L00, PREF_CODE_10G00,
           |PREF_CODE_10A00, MIP_SEQ_ID, STUDENT_FLG, IBMER_FLG, STATE_CD FROM
           |MAP_MKTO.MCT_MKTO_PERSON p
           |WHERE
           |STATUS_CODE = 'U' AND
           |IDM_ID IS NOT NULL AND
           |EMAIL_MEDIA_ID IS NOT NULL AND
           |EMAIL_ADDR IS NOT NULL AND
           |RIGHT(MIP_SEQ_ID, 1) IN ($partitionNo) AND
           |EMAIL_MEDIA_ID != -1
           |ORDER BY 2 asc),
           |etl_config_data AS (
           |SELECT
           |$minBatchSize record_limit,
           |$elapsedTime elapsed_time_limit_in_mins ,
           |'$lastRunTimestamp' AS last_sync_timestamp
           |FROM sysibm.sysdummy1)
           |SELECT *
           |FROM
           |ranked_data,
           |etl_config_data
           |WHERE
           |record_limit <= (
           |SELECT
           |count(1)
           |FROM ranked_data)
           |OR timestampdiff( 4, CURRENT timestamp - last_sync_timestamp ) > elapsed_time_limit_in_mins
           |FETCH FIRST $maxThreshold ROWS ONLY)""".stripMargin

      print(sqlData)
      val conProp1: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
      val dfResult1 = AppProperties.SparkSession.read.jdbc(conProp1.getProperty(PropertyNames.EndPoint), sqlData, conProp1)
      dfResult1.show(false)


      count = dfResult1.count()
      var counter = count

      val dbConnectionInfo = conProp1.getProperty(PropertyNames.EndPoint)
      dbCon = DriverManager.getConnection(dbConnectionInfo, conProp1)

      if(count > 0) {

        log.info("Reading unprocessed data")
        dfResult1.show(false)

        //Get configuration from ETL_DATA_SOURCE
        val appProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, configSource)

        //Mapping which will be used to map marketo to DB
        val columnMapping = appProp.getProperty(PropertyNames.ResourceSpecific_2).stripMargin.replaceAll("\\s+", "")

        /*Updated - 08-10-2021
        val columnMapping ="""IDM_ID=IDM_ID,
                              |EMAIL_MEDIA_ID=Email_Media_ID,
                              |NEW_TO_IDM_IND=New_To_IDM_Flag,
                              |FIRST_NAME=firstName,
                              |LAST_NAME=lastName,
                              |EMAIL_ADDR=email,
                              |COMPANY_NAME=company,
                              |CTRY_CODE=CountryCode,
                              |IDM_COMPANY_ID=IDM_Company_ID,
                              |DQ_EMAIL_IND=DQ_Email_Flag,
                              |DQ_NAME_IND=DQ_Name_Flag,
                              |DQ_PHONE_IND=DQ_Phone_Flag,
                              |IDM_FEDGOV_IND=FedGov_Flag,
                              |PREF_CODE_IBM=PrefCode_IBM,
                              |COMPANY_PHONE_SUPR_CODE=Company_Phone_Suppression,
                              |COMPANY_EMAIL_SUPR_CODE=Company_Email_Suppression,
                              |PREF_CODE_17YNI=PrefCode_17YNI,
                              |PREF_CODE_17ENL=PrefCode_17ENL,
                              |PREF_CODE_17DSR=PrefCode_17DSR,
                              |PREF_CODE_17CPH=PrefCode_17CPH,
                              |PREF_CODE_17BCH=PrefCode_17BCH,
                              |PREF_CODE_17AAL=PrefCode_17AAL,
                              |PREF_CODE_15WSC=PrefCode_15WSC,
                              |PREF_CODE_15WCP=PrefCode_15WCP,
                              |PREF_CODE_15STT=PrefCode_15STT,
                              |PREF_CODE_15MFT=PrefCode_15MFT,
                              |PREF_CODE_15ITT=PrefCode_15ITT,
                              |PREF_CODE_15IGO=PrefCode_15IGO,
                              |PREF_CODE_15CLV=PrefCode_15CLV,
                              |PREF_CODE_153QH=PrefCode_153QH,
                              |PREF_CODE_10N00=PrefCode_10N00,
                              |PREF_CODE_10M00=PrefCode_10M00,
                              |PREF_CODE_10L00=PrefCode_10L00,
                              |PREF_CODE_10G00=PrefCode_10G00,
                              |PREF_CODE_10A00=PrefCode_10A00,
                              |MIP_SEQ_ID=MIP_Person_Seq_ID""".stripMargin.replaceAll("\\s+", "")

         */

        log.info("Creating the column mapping")
        val mapDfToDBColumns: mutable.Map[String, String] = mutable.Map[String, String]()
        for (curVal <- columnMapping.split(",")) {
          val arrVal = curVal.split("=")
          mapDfToDBColumns += arrVal(0) -> arrVal(1)
        }

        println(mapDfToDBColumns)
        var newDF = dfResult1.select(mapDfToDBColumns.keys.toList.distinct.head, mapDfToDBColumns.keys.toList.distinct.tail: _*)
        val newCollist = mapDfToDBColumns.keys.toList

        for (i <- newCollist) {
          newDF = newDF.withColumnRenamed(i,mapDfToDBColumns(i))
        }

        log.info("Trimming the columns")
        val trimColumns=newDF.schema.fields.filter(_.dataType.isInstanceOf[StringType])
        trimColumns.foreach(f=>{
          newDF=newDF.withColumn(f.name,trim(col(f.name)))
          newDF = newDF.withColumn(f.name,regexp_replace(col(f.name), "\"", ""))
          newDF = newDF.withColumn(f.name,regexp_replace(col(f.name), "[\\,]", ""))
          newDF = newDF.withColumn(f.name,regexp_replace(col(f.name), "[\\p{C}]", ""))
        })

        val spark = AppProperties.SparkSession
        import spark.implicits._

        log.info("Creating the dataframe for the payload")
        val limitDF = newDF.dropDuplicates("IDM_ID").persist()
        log.info("Preparing for POST")
        log.info("Updating status for PRE-POST")
        mip_seq_id = updatePrePostStatusv2(limitDF,conProp1,tgtTableName)
        revertDF = limitDF
        val payload = buildPayload(limitDF, action, lookupField)
        val jsValue = Json.parse(payload)
        Json.prettyPrint(jsValue)

        log.info("Payload to be sent to marketo")
        println(payload)

        //Sends post to marketo
        var response = sendPost(payload)

        log.info("API Response")
        log.info(response)

        if (response.contains("Access token expired") || response.contains("Access token invalid")) {

          val newToken = getMarketoToken
          response = sendPostWithToken(payload, newToken)

        }

        log.info("Parsing the response from Marketo")
        //Parsing the response to a Dataframe to extract lead id
        val parsedJson = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(response)).toDS())

        //Generate the dataframe to update to the source table
        var testDF = parsedJson.select(explode(col("result")).as("result")).select("result.*")
        testDF.show(false)

        lead_id = testDF.select("id").as[leadId].collect()


        log.info("Creating the mapping for Lead ID and MIP SEQ ID")
        for(i <- mip_seq_id.indices) {
          val leadId: Long = if (lead_id(i).id == null) {
            counter = counter - 1
            errorCounter = "-"+ counter.toString
            errorCounter.toLong
          } else {
            lead_id(i).id.toLong
          }
          val mipSeqIdList = mip_seq_id(i).MIP_Person_Seq_ID
          mapIds += leadId -> mipSeqIdList

        }

        println(mapIds)
        val mipSeqIDList = mapIds.values.toString().substring(7,mapIds.values.toString().length)
        val postDF = mapIds.toSeq.toDF("leadId","mipSeqID")

        postDF.show(false)

        log.info("Updating the lead id's")
        //Function which will update the LeadID for the processed messages
        updatePostStatusv2(postDF,dbCon,tgtTableName)
        updateErrorStatus(postDF,dbCon,tgtTableName)

        log.info("Inserting into MAP_MKTO.MCT_MKTO_LEAD_XREF")
        val sqlData2 =
          s"""(SELECT MIP_SEQ_ID, MKTO_LEAD_ID, EMAIL_ADDR, IDM_ID, EMAIL_MEDIA_ID, 'N' AS PROCESSED_FLG, CAST(NULL as timestamp) AS PROCESSED_TS, CURRENT_TIMESTAMP AS TRANSACTION_TS, CURRENT_TIMESTAMP AS CREATE_TS, CURRENT_TIMESTAMP AS UPDATE_TS, CREATE_USER FROM MAP_MKTO.MCT_MKTO_PERSON
             |WHERE
             |MIP_SEQ_ID IN $mipSeqIDList AND STATUS_CODE = 'P')""".stripMargin

        println(sqlData2)

        val conProp2: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
        val dfResult2 = AppProperties.SparkSession.read.jdbc(conProp1.getProperty(PropertyNames.EndPoint), sqlData2, conProp2)

        dfResult2.show(false)

        insertAfterPost(dfResult2,dbCon,"MAP_MKTO.MCT_MKTO_LEAD_XREF")


        /* Commenting out the insert to MAP_IDM.IDM_MAINTAIN_PERSON
        val sqlData3 =
          s"""(SELECT TRANSACTION_TS AS ORIG_TRANS_TS, 'MARKETO' AS DATA_SOURCE, MIP_SEQ_ID AS ORIG_TRANS_ID, 'EID' AS PERSON_IDENTIFIER_TYPE, IDM_ID AS PERSON_IDENTIFIER_VALUE, 'MARKETO' AS PERSON_ADMIN_SYSTEM, MKTO_LEAD_ID AS PERSON_ADMIN_ID FROM MAP_MKTO.MCT_MKTO_LEAD_XREF
             |WHERE
             |EMAIL_MEDIA_ID IN $emailIdList)""".stripMargin

        println(sqlData3)

        val conProp3: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
        val dfResult3 = AppProperties.SparkSession.read.jdbc(conProp1.getProperty(PropertyNames.EndPoint), sqlData3, conProp3)

        insertAfterPostIDM(dfResult3,dbCon,"MAP_IDM.IDM_MAINTAIN_PERSON")
        */

      }
      else{
        log.info("no records present to process")
      }

    }
    catch
    {
      case e: Throwable =>
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        val tgtProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
        if(!revertDF.isEmpty)
        {
          revertPrePostStatusv2(revertDF,tgtProp,tgtTableName)
        }
        bException = true
    }
    finally
    {
      if (bException) { // Job failed
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobFailed, s"e.getMessage - e.getCause", null, null)
        exit(1)
      } else {
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobSucceeded, "Job completed without any error", null, null)
      }
      log.info("Closing db connections")
      dbCon.close()
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }

  //Case class definition to extract from JSON response
  //case class resultPost(id: String, status: String)
  case class ResultPost(no: String, status: String, reasons: String)
  case class ResultResponse(requestId: String, result: ResultPost, success: String)
  case class mipSeqId(MIP_Person_Seq_ID: Long) //NOSONAR
  case class leadId(id: String) //NOSONAR


  implicit val readPost: Reads[ResultPost] = (
    (JsPath \ "no").read[String] and
      (JsPath \ "status").read[String] and
      (JsPath \ "reasons").read[String]
    ) (ResultPost.apply _)

  implicit val cert: Reads[ResultResponse] = (
    (JsPath \ "requestId").read[String] and
      (JsPath \ "result").read[ResultPost] and
      (JsPath \ "success").read[String]
    ) (ResultResponse.apply _)

  //Updating data status after posting it in marketo
  def updatePrePostStatus(dataFrame: DataFrame, properties: Properties, updateTableName: String): ArrayBuffer[Long] = {

    var dbCon: Connection = null
    var stmt: Statement = null
    var name = ArrayBuffer[Long]()

    dataFrame.collect().foreach { row =>
      val emailMediaID = row.getLong(row.fieldIndex("Email_Media_ID"))
      name += emailMediaID
      val updateSql2 = s"""UPDATE $updateTableName SET STATUS_CODE = 'I' WHERE EMAIL_MEDIA_ID=$emailMediaID""".stripMargin
      dbCon = DriverManager.getConnection(properties.getProperty(PropertyNames.EndPoint), properties)
      stmt = dbCon.createStatement
      stmt.executeUpdate(updateSql2)
      dbCon.commit()
    }

    name
  }

  def updatePrePostStatusv2(dataFrame: DataFrame, properties: Properties, updateTableName: String): Array[mipSeqId] = {
    val dfUpd = dataFrame.select("MIP_Person_Seq_ID").toDF()
    DataUtilities.runPreparedStatement(
      properties,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = 'I' WHERE MIP_SEQ_ID = ?""",
      dfUpd.columns,Array(0),null,"UPDATE")

    val spark = AppProperties.SparkSession
    import spark.implicits._
    var name = Array[mipSeqId]()
    name = dataFrame.select("MIP_Person_Seq_ID").as[mipSeqId].collect()
    name
  }

  def revertPrePostStatusv2(dataFrame: DataFrame, properties: Properties, updateTableName: String): Unit = {
    val dfUpd = dataFrame.select("MIP_Person_Seq_ID").toDF()
    DataUtilities.runPreparedStatement(
      properties,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = 'U' WHERE MIP_SEQ_ID = ? and STATUS_CODE = 'I'""",
      dfUpd.columns,Array(0),null,"UPDATE")
  }

  //Updating data status after posting it in marketo
  def updatePostStatus(properties: Properties, updateTableName: String, mapIds: scala.collection.mutable.Map[Long, Long]): Unit = {
    var dbCon: Connection = null
    var stmt: Statement = null

    for((key,value) <- mapIds){
      val updateSql2 = s"""UPDATE $updateTableName SET MKTO_LEAD_ID = $value, STATUS_CODE = 'P' WHERE EMAIL_MEDIA_ID=$key""".stripMargin
      dbCon = DriverManager.getConnection(properties.getProperty(PropertyNames.EndPoint), properties)
      stmt = dbCon.createStatement
      stmt.executeUpdate(updateSql2)
      dbCon.commit()
    }
  }

  def updatePostStatusv2(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {

    val dfUpd = dataFrame.select("leadId","mipSeqID").toDF()

    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET MKTO_LEAD_ID = ?, STATUS_CODE = 'P', ERROR_CODE = NULL, ERROR_DESC = NULL WHERE MIP_SEQ_ID= ?""",
      dfUpd.columns,Array(0,1),null,true,updateTableName,"UPDATE")

  }

  def updateErrorStatus(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {

    val dfUpd = dataFrame.select("mipSeqID").toDF()

    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = 'E', ERROR_CODE = 'INVALID', ERROR_DESC = 'CHECK PAYLOAD', MKTO_LEAD_ID = NULL WHERE MKTO_LEAD_ID < 0 AND MIP_SEQ_ID= ?""",
      dfUpd.columns,Array(0),null,true,updateTableName,"UPDATE")

  }

  def insertAfterPost(dataFrame: DataFrame, dbConn: Connection, insertTableName: String): Unit = {

    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dataFrame,
      s"""INSERT INTO $insertTableName (MIP_SEQ_ID, MKTO_LEAD_ID, EMAIL_ADDR, IDM_ID, EMAIL_MEDIA_ID, PROCESSED_FLG, PROCESSED_TS, TRANSACTION_TS, CREATE_TS, UPDATE_TS, CREATE_USER) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
      dataFrame.columns,Array(0,1,2,3,4,5,6,7,8,9,10),
      null,
      true,insertTableName,
      "INSERT")

  }

  def insertAfterPostIDM(dataFrame: DataFrame, dbConn: Connection, insertTableName: String): Unit = {

    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dataFrame,
      s"""INSERT INTO $insertTableName (ORIG_TRANS_TS, DATA_SOURCE, ORIG_TRANS_ID, PERSON_IDENTIFIER_TYPE, PERSON_IDENTIFIER_VALUE, PERSON_ADMIN_SYSTEM, PERSON_ADMIN_ID) VALUES (?,?,?,?,?,?,?)""",
      dataFrame.columns,Array(0,1,2,3,4,5,6),
      null,
      false,insertTableName,
      "INSERT")

  }


  @throws(classOf[Exception])
  def getData(sqlData: String): DataFrame = {
    val conProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
    var dfResult: DataFrame = null
    try {
      dfResult = AppProperties.SparkSession.read
        .jdbc(conProp.getProperty(PropertyNames.EndPoint), sqlData,
          conProp)

    } finally {
      if (dfResult != null) dfResult.unpersist()
    }

    dfResult
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