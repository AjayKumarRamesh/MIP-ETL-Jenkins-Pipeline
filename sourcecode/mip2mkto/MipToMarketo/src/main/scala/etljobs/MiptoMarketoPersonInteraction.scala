package etljobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.exception.RestServiceException
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.sql.functions.{to_json, _}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.JArray
import org.json4s.jackson.JsonMethods.parse
import play.api.libs.json.{Json}

import java.net.SocketTimeoutException
import java.sql.{Connection, DriverManager, Statement, Timestamp}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object MiptoMarketoPersonInteraction extends ETLFrameWork {

  //Initialization of variables
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var tgtTableNameP: String = null
  var tgtTableNameCA: String = null
  var tgtImiTableName: String = null
  var tgtXrefTableName: String = null
  var apiSourcePerson: String = null
  var apiSourceInteraction: String = null
  var dbSource: String = null
  var configSourcePerson: String = null
  var configSourceInteraction: String = null
  var bException: Boolean = false
  var minBatchSize: Int = 0
  var maxThreshold: Int = 0
  var elapsedTime: Int = 0
  var action: String = null
  var hrmDeployedDate: String = null
  var hrmSequenceNumber: Long = 0
  var contactInteraction: Long = 0
  var payloadInteraction: String = null
  var errorStatusPerson: String = null
  var errorStatusCA: String = null
  var successStatus: String = null
  var errString: String = null
  var successString: String = null

  override def getPropertiesFromJson(json: String): Properties = super.getPropertiesFromJson(json)
  override def getDataSourceDetails(spark: SparkSession, dataSourceCode: String): Properties = super.getDataSourceDetails(spark, dataSourceCode)

  var lookupFieldPerson: String = null
  var lookupFieldInteraction: String = null
  var finalJoinedDFCount: Long = 0
  var count: Long = 0
  var customCount: Long = 0
  var customSuccessCount: Long = 0
  var errorCounter: String = null
  var personCountFailed: Long = 0
  var personSuccessCount: Long = 0
  var exceptionMessage: String = null

  // CASE CLASSES FOR PERSON
  //Case class definition to extract from JSON response
  //case class resultPost(id: String, status: String)
  case class resultPost(no: String, status: String, reasons: String)
  case class resultResponse(requestId: String, result: resultPost, success: String)
  case class mipSeqId(MIP_Person_Seq_ID: Long)
  case class inboundMktgId(INBOUND_MKTG_ID: Long)
  case class leadId(id: String)

  // CASE CLASSES FOR CUSTOM ACTIVITY
  //Case class definition to extract from JSON response
  //case class resultPost(id: String, status: String)
  case class mipActSeqId(MIP_ACTIVITY_SEQ_ID: Long)
  case class guID(id: Option[Long] = null)
  case class emailID(interactionID: JArray)
  case class resultErrorPost(code: String, message: String)
  case class resultErrorResponse(requestId: String, result: resultErrorPost, success: String)

  //Gets marketo token to perform post to PERSON
  def getMarketoTokenPerson: String = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSourcePerson)
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
  def buildPayloadPerson(transformedDF: DataFrame, action: String, lookUpField: String): String = {
    val output_df = transformedDF.select(to_json(struct(col("*"))).alias("content"))
    val testData = output_df.rdd.map(row => row.getString(0)).collect
    val inputData: String = testData.mkString(",")
    val strNew1 = inputData.replaceAll("[\"][a-zA-Z0-9_]*[\"]:\"\"[,]?", "")
    val finalStr = strNew1.replaceAll("[,]?}", "}")
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
  def sendPostPerson(payload: String): String = {

    val apiConProps: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSourcePerson)
    val externalEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
    var postResponse: String = null
    val httpPostToken = null
    val httpClient: CloseableHttpClient = HttpClients.custom().build()

    //Exception handling for duplicates in Payload
    try {
      val token = getMarketoTokenPerson
      val httpPostToken = new HttpPost(s"$externalEndpoint?access_token=$token")
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

  //Gets marketo token to perform post to Interaction
  def getMarketoTokenInteraction: String = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSourceInteraction)
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

  def convert(ts: String): String = {
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val date = df1.parse(ts)
    val epoch = date.getTime
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
    df.format(epoch)
  }

  def removeControlChar(ts: String): String = {
    var newVal: String = null
    if (ts == null || ts == "") {
      ts
    }
    else {
      newVal = ts.replaceAll("[\\p{C}]", "").replaceAll("[\\r\\n\\t]", "")
      val finalVal = newVal.replaceAll("\"", "")
      finalVal
    }
  }

  def buildPayloadInteraction(transformedDF: DataFrame): DataFrame = {

    val spark = AppProperties.SparkSession
    var activityDF1: DataFrame = null
    //${convert({row.getTimestamp(25).toString})}

    import spark.implicits._

    activityDF1 = transformedDF.filter(transformedDF("Activity_Type") === contactInteraction).map(row => {
      s"""{
         |      "leadId":${row.getLong(9)},
         |      "activityDate":"${convert({row.getTimestamp(31).toString})}",
         |      "activityTypeId":${row.getInt(19)},
         |      "primaryAttributeValue": "${row.getString(25)}",
         |      "attributes":
         |          [
         |              {
         |                    "apiName": "Content_Campaign_Name",
         |                    "value": "${removeControlChar(row.getString(24))}"
         |                },
         |                {
         |                    "apiName": "Form_Name",
         |                    "value": "${removeControlChar(row.getString(35))}"
         |                },
         |                {
         |                    "apiName": "GBL_IMT_CD",
         |                    "value": "${removeControlChar(row.getString(7))}"
         |                },
         |                {
         |                    "apiName": "GBL_IOT_CD",
         |                    "value": "${removeControlChar(row.getString(15))}"
         |                },
         |                {
         |                    "apiName": "GBL_RGN_CD",
         |                    "value": "${removeControlChar(row.getString(21))}"
         |
         |                },
         |                {
         |                    "apiName": "Interaction_ID",
         |                    "value": "${row.getLong(33)}"
         |
         |                },
         |                {
         |                    "apiName": "Lead_Description",
         |                    "value": "${removeControlChar(row.getString(27))}"
         |
         |                },
         |                {
         |                    "apiName": "Lead_Note",
         |                    "value": "${removeControlChar(row.getString(10))}"
         |
         |                },
         |                {
         |                    "apiName": "Lead_Source",
         |                    "value": "${removeControlChar(row.getString(37))}"
         |
         |                },
         |                {
         |                    "apiName": "Person_Country_Code",
         |                    "value": "${removeControlChar(row.getString(3))}"
         |
         |                },
         |                {
         |                    "apiName": "Person_Phone",
         |                    "value": "${removeControlChar(row.getString(8))}"
         |
         |                },
         |                {
         |                    "apiName": "Sales_Channel",
         |                    "value": "${removeControlChar(row.getString(16))}"
         |
         |                },
         |                {
         |                    "apiName": "Strength",
         |                    "value": "${row.getLong(1)}"
         |
         |                },
         |                {
         |                    "apiName": "UT10_Code",
         |                    "value": "${removeControlChar(row.getString(30))}"
         |
         |                },
         |                {
         |                    "apiName": "UT15_Code",
         |                    "value": "${removeControlChar(row.getString(28))}"
         |
         |                },
         |                {
         |                    "apiName": "UT17_Code",
         |                    "value": "${removeControlChar(row.getString(4))}"
         |
         |                },
         |                {
         |                    "apiName": "UT20_Code",
         |                    "value": "${removeControlChar(row.getString(26))}"
         |
         |                },
         |                {
         |                    "apiName": "UT30_Code",
         |                    "value": "${removeControlChar(row.getString(13))}"
         |
         |                },
         |                {
         |                    "apiName": "Create_TS",
         |                    "value": "${DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").format(LocalDateTime.now)}"
         |
         |                },
         |                {
         |                    "apiName": "MIP_Activity_Seq_ID",
         |                    "value": "${row.getLong(14)}"
         |                },
         |                {
         |                    "apiName": "Item_URL",
         |                    "value": "${removeControlChar(row.getString(23))}"
         |
         |                }
         |          ]
         |}""".stripMargin

    }).toDF().persist()

    activityDF1
    val finalDF = Seq(activityDF1)

    finalDF.reduce(_ union _)
  }

  //Function to perform POST to Marketo
  def sendPostInteraction(payload: String): String = {

    val apiConProps: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSourceInteraction)
    val externalEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
    var postResponse: String = null
    val httpPostToken = null
    val httpClient: CloseableHttpClient = HttpClients.custom().build()

    //Exception handling for duplicates in Payload
    try {
      val token = getMarketoTokenInteraction
      val httpPostToken = new HttpPost(s"$externalEndpoint?access_token=$token")
      httpPostToken.addHeader("Content-type", "application/json;charset=UTF-8")
      httpPostToken.addHeader("Accept", "application/json")
      httpPostToken.setEntity(new StringEntity(payload, "UTF-8"))
      postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())
      if (postResponse.contains("skipped")) {
        if (postResponse.contains("Lead already exists")) {
          throw RestServiceException("Lead Already exists. Kindly check the action value in the Payload. Try with action:createOrUpdate")
        }
        if (postResponse.contains("Multiple lead match lookup criteria")) {
          throw RestServiceException("Please check the lookup variable, it may contain duplicates")
        }
      }
    }
    catch {
      // Case statement
      case _: SocketTimeoutException =>
        //Second Call
        postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())
    }
    postResponse
  }

    def sendPostWithTokenInteraction(payload: String, token: String): String = {

      val apiConProps: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSourceInteraction)
      val externalEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
      var postResponse: String = null
      val httpPostToken = null
      val httpClient: CloseableHttpClient = HttpClients.custom().build()

      //Exception handling for duplicates in Payload
      try {
        val httpPostToken = new HttpPost(s"$externalEndpoint?access_token=$token")
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


    def main(args: Array[String]): Unit = {
      //Args to the job
      tgtTableNameP = args(args.indexOf("--tgtTablePerson") + 1)
      tgtTableNameCA = args(args.indexOf("--tgtTableInteraction") + 1)
      tgtImiTableName = args(args.indexOf("--tgtImiTable") + 1)
      tgtXrefTableName = args(args.indexOf("--tgtXrefTable") + 1)
      apiSourcePerson = args(args.indexOf("--apiSourcePerson") + 1)
      apiSourceInteraction = args(args.indexOf("--apiSourceInteraction") + 1)
      dbSource = args(args.indexOf("--dbSource") + 1)
      configSourcePerson = args(args.indexOf("--configSourcePerson") + 1)
      configSourceInteraction = args(args.indexOf("--configSourceInteraction") + 1)
      minBatchSize = args(args.indexOf("--minBatchSize") + 1).toInt
      maxThreshold = args(args.indexOf("--maxThreshold") + 1).toInt
      elapsedTime = args(args.indexOf("--elapsedTime") + 1).toInt
      action = args(args.indexOf("--action") + 1)
      lookupFieldPerson = args(args.indexOf("--lookupFieldPerson") + 1)
      lookupFieldInteraction = args(args.indexOf("--lookupFieldInteraction") + 1)
      hrmDeployedDate = args(args.indexOf("--hrmDeployedDate") + 1)
      hrmSequenceNumber = args(args.indexOf("--hrmSequenceNumber") + 1).toLong
      contactInteraction = args(args.indexOf("--contactInteraction") + 1).toLong

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")
      log.info(s"Starting ETL Job => $jobClassName....")
      val jobSequence = s"$jobClassName"

      //Variables From PERSON Code
      val last_run_timestamp = getMaxRecordTimestampTest(jobSequence)
      println(last_run_timestamp)
      var mip_seq_id = Array[MiptoMarketoPersonInteraction.mipSeqId]()
      var lead_id = Array[MiptoMarketoPersonInteraction.leadId]()
      var dbCon: Connection = null
      var mapPersonIds = scala.collection.mutable.Map[Long, Long]()
      var personFlag = 0
      var errorCodePerson: String = null
      var errorDescPerson: String = null

      //Variables From Custom Activity Code
      var interIds = scala.collection.mutable.Map[Int, Long]()
      var mip_act_seq_id = Array[MiptoMarketoPersonInteraction.mipActSeqId]()
      var marketoID = Array[guID]()
      val sortedmipActSeqId = ArrayBuffer[Option[Long]]()
      var interactionFlag = 0
      var errorCodeCA: String = null
      var errorDescCA: String = null
      var mapInteractionIds = scala.collection.mutable.Map[Option[Long], Long]()

      // Logging Job Status in ETLJobHistory Table
      DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobStarted, "GOOD LUCK", null, null)
      log.info("ETL logic goes here...")

      // Reading the Required Data from the Source Table (IMI)
      try {
        val combinedSqlQuery =
          s"""(WITH CTE1 AS(SELECT
             |IMI.INBOUND_MKTG_ID AS INBOUND_MKTG_ID,
             |IMI.IDM_ID,
             |IMI.EMAIL_MEDIA_ID,
             |CASE
             |WHEN COALESCE(IDM.IDM_PERSON_CREATED_TS, IDM.CREATE_TS) > (CURRENT_TIMESTAMP - 24 HOURS)
             |THEN 'T'
             |ELSE 'F'
             |END AS NEW_TO_IDM_IND,
             |RRG.FIRSTNAME AS FIRST_NAME,
             |RRG.LASTNAME AS LAST_NAME,
             |IMI.EMAIL_ADDR,
             |IMI.COMPANY_NAME,
             |IMI.PERSON_CTRY_CD AS CTRY_CODE,
             |IMI.IDM_COMPANY_ID,
             |CASE
             |WHEN IDM.EMAIL_OC = '100001'
             |THEN 'N'
             |ELSE
             |CASE
             |WHEN IDM.EMAIL_OC IS NULL
             |AND ( IDM.IDM_EMAIL_ADDRESS_CC IS NOT NULL
             |AND IDM.IDM_EMAIL_ADDRESS_CC NOT IN (999999, 999999999))
             |THEN 'N'
             |ELSE 'Y'
             |END
             |END AS DQ_EMAIL_IND,
             |CASE
             |WHEN IDM.PERSON_NAME_OC IS NULL
             |AND ( IDM.IDM_PERSON_NAME_CC IS NOT NULL
             |AND IDM.IDM_PERSON_NAME_CC NOT IN (999999, 999999999))
             |THEN 'N'
             |ELSE 'Y'
             |END AS DQ_NAME_IND,
             |CASE
             |WHEN IDM.PHONE_1_OC = '100001'
             |THEN 'N'
             |ELSE
             |CASE
             |WHEN IDM.PHONE_1_OC IS NULL
             |AND (IDM.IDM_PHONE_1_CC IS NOT NULL
             |AND IDM.IDM_PHONE_1_CC NOT IN (999999, 999999999))
             |THEN 'N'
             |ELSE 'Y'
             |END
             |END AS DQ_PHONE_IND,
             |IMI.CREATE_TS AS CREATE_TS,
             |IDM.IDM_FEDGOV_FLAG AS IDM_FEDGOV_IND,
             |RSP.COMP_EMAIL_SP AS COMPANY_EMAIL_SUPR_CODE,
             |RSP.COMP_PHONE_SP AS COMPANY_PHONE_SUPR_CODE,
             |PRF.PREF_CODE_IBM,
             |PRF.PREF_CODE_10A00,
             |PRF.PREF_CODE_10G00,
             |PRF.PREF_CODE_10L00,
             |PRF.PREF_CODE_10M00,
             |PRF.PREF_CODE_10N00,
             |PRF.PREF_CODE_153QH,
             |PRF.PREF_CODE_15CLV,
             |PRF.PREF_CODE_15IGO,
             |PRF.PREF_CODE_15ITT,
             |PRF.PREF_CODE_15MFT,
             |PRF.PREF_CODE_15STT,
             |PRF.PREF_CODE_15WCP,
             |PRF.PREF_CODE_15WSC,
             |PRF.PREF_CODE_17AAL,
             |PRF.PREF_CODE_17BCH,
             |PRF.PREF_CODE_17CPH,
             |PRF.PREF_CODE_17DSR,
             |PRF.PREF_CODE_17ENL,
             |PRF.PREF_CODE_17YNI,
             |PRF.PREF_CODE_15S8X,
             |CASE
             |WHEN UPPER(RRG.JOBTITLE) = 'STUDENT'
             |OR UPPER(IDM.person_title) LIKE '%STUDENT%'
             |OR UPPER(Q_STUDENT) = 'YES'
             |OR UPPER(RRG.ATTENDEETYPE) = 'STUDENT'
             |THEN '1'
             |ELSE '0'
             |END AS STUDENT_FLG,
             |CASE
             |WHEN RRG.IBMUSER = 1
             |OR IDM.IDM_PERSON_IBM_FLAG = 'Y'
             |THEN '1'
             |ELSE '0'
             |END AS IBMer_FLG,
             |CASE
             |WHEN IMI.STATE_CD = ''
             |THEN NULL
             |ELSE IMI.STATE_CD
             |END AS STATE_CD,
             |RP_PHONE1.sp_type AS WORK_PHONE_PERM,
             |COP.SAP_CUST_NUM,
             |'P' AS STATUS_CODE,
             |CAST (NULL AS VARCHAR) AS ERROR_CODE,
             |CAST (NULL AS VARCHAR) AS ERROR_DESC,
             |IMI.DATA_SRC_DESC AS MIP_TRANS_SRC,
             |IMI.REGISTRATION_ID AS MIP_TRANS_ID,
             |CAST (NULL AS VARCHAR) AS MKTO_PARTITION_ID,
             |IMI.READY_FOR_MKTO_FLG,
             |CASE
             |WHEN IMI.MARKETING_INTERACTION_TYPE_CD IN ('MAIL')
             |THEN ACT_ID.CONTACT_INTERACTION_TYPE_ID
             |END AS ACTIVITY_TYPE_ID,
             |IMI.CONTENT_CMPN_CD AS CAMPAIGN_CODE,
             |IMI.USER_TRANSACTION_TS AS ACTIVITY_TS,
             |IMI.CONTENT_CMPN_NAME AS CAMPAIGN_NAME,
             |IMI.ASSET_CTRY_CODE AS COUNTRY_CODE,
             |IMI.ASSET_LANG_CODE AS LANG_CODE,
             |IMI.ASSET_DEFAULT_TITLE AS ACTIVITY_NAME,
             |IMI.ASSET_CONTENT_TYPE AS ASSET_TYPE,
             |IMI.ASSET_DLVRY_URL AS ACTIVITY_URL,
             |IMI.UUC_ID AS UUC_ID,
             |IMI.DRIVER_CMPN_CD AS DRIVER_CAMPAIGN_CODE,
             |IMI.SUB_SRC_DESC AS FORM_NAME,
             |IMI.INBOUND_MKTG_ID AS INTERACTION_ID,
             |IMI.MKTO_QUEUED_TS AS INTERACTION_TS,
             |IMI.SUB_SRC_DESC,
             |IMI.LEAD_DESC,
             |CASE WHEN IMI.MARKETING_INTERACTION_TYPE_CD = ('MAIL') THEN SUBSTRING(TRIM(IMI.LEAD_NOTE) || ' ' ||
             |CASE WHEN (Q_HELP = '' or Q_HELP IS NULL) THEN '' ELSE NVL(Q_HELP||' ','') END ||
             |CASE WHEN (Ans_Q_QRADAR_BP = '' or Ans_Q_QRADAR_BP IS NULL) THEN '' ELSE NVL('| Business Partner='||Ans_Q_QRADAR_BP||' ','') END ||
             |CASE WHEN (Ans_Q_MAIL_CONSENT = '' or Ans_Q_MAIL_CONSENT IS NULL) THEN '' ELSE NVL('| Q_MAIL_CONSENT='||Ans_Q_MAIL_CONSENT||' ','') END ||
             |CASE WHEN (Ans_Q_QRDATA = '' or Ans_Q_QRDATA IS NULL) THEN '' ELSE NVL('| QRader Estimator values='||Ans_Q_QRDATA||' ','') END,1,3072)
             |END AS LEAD_NOTE,
             |IMI.LEAD_SRC AS LEAD_SRC_NAME,
             |IMI.MARKETING_INTERACTION_TYPE_CD AS INTERACTION_TYPE_CODE,
             |IMI.PHONE AS CONTACT_PHONE,
             |IMI.SALES_CHANNEL_CD AS SALES_CHANNEL_NAME,
             |IMI.SCORE_TS,
             |IMI.IND_STRENGTH_NUM AS STRENGTH,
             |CASE
             |WHEN IMI.MARKETING_INTERACTION_TYPE_CD IN ('MAIL')
             |THEN IMI.CONTENT_CMPN_UT10_CD
             |WHEN IMI.SCORE_UT10_CD IS NOT NULL
             |THEN IMI.SCORE_UT10_CD
             |WHEN IMI.ACTIVITY_CMPN_UT10_CD IS NOT NULL
             |THEN IMI.ACTIVITY_CMPN_UT10_CD
             |ELSE IMI.CONTENT_CMPN_UT10_CD
             |END AS UT10_CODE,
             |CASE
             |WHEN IMI.MARKETING_INTERACTION_TYPE_CD IN ('MAIL')
             |THEN IMI.CONTENT_CMPN_UT15_CD
             |WHEN IMI.SCORE_UT10_CD IS NOT NULL
             |THEN IMI.SCORE_UT15_CD
             |WHEN IMI.ACTIVITY_CMPN_UT10_CD IS NOT NULL
             |THEN IMI.ACTIVITY_CMPN_UT15_CD
             |ELSE IMI.CONTENT_CMPN_UT15_CD
             |END AS UT15_CODE,
             |CAST ('NULL' AS VARCHAR(5)) AS UT17_CODE,
             |CASE
             |WHEN IMI.MARKETING_INTERACTION_TYPE_CD IN ('MAIL')
             |THEN IMI.CONTENT_CMPN_UT20_CD
             |WHEN IMI.SCORE_UT10_CD IS NOT NULL
             |THEN IMI.SCORE_UT20_CD
             |WHEN IMI.ACTIVITY_CMPN_UT10_CD IS NOT NULL
             |THEN IMI.ACTIVITY_CMPN_UT20_CD
             |ELSE IMI.CONTENT_CMPN_UT20_CD
             |END AS UT20_CODE,
             |IMI.SCORE_UT30_CD AS UT30_CODE,
             |CAST (NULL AS VARCHAR(255)) AS NEXT_COMM_METHOD_NAME,
             |CAST (NULL AS TIMESTAMP) AS NEXT_COMM_TS,
             |CAST (NULL AS VARCHAR(255)) AS NEXT_KEYWORDS,
             |CAST (NULL AS VARCHAR(50)) AS NEXT_UUC_ID,
             |CAST (NULL AS VARCHAR(256)) AS WEB_PAGE_SRC,
             |CAST (NULL AS BIGINT) AS MKTO_ACTIVITY_ID,
             |IMI.EVENT_REF_ID,
             |IMI.REFERRER_URL AS REFERRER_URL,
             |IMI.ACTIVITY_CMPN_CD AS ACTIVITY_CMPN_CD,
             |MC.IBM_GBL_IOT_CODE,
             |MC.SUB_REGION_CODE,
             |MC.REGION
             |FROM
             |MAP_CORE.MCT_INBOUND_MARKETING_INTERACTION_MIP IMI
             |LEFT JOIN
             |MAP_IDM.IDM_MAINTAIN_PERSON IDM ON
             |IMI.REGISTRATION_ID = IDM.MAT_TRANSACTIONID
             |AND IMI.data_src_desc = IDM.data_source
             |LEFT JOIN
             |MAP_STG.STG_RAW_REGISTRATION RRG ON
             |IDM.MAT_TRANSACTIONID = RRG.TRANSACTIONID
             |AND IMI.data_src_desc = RRG.DATASOURCE
             |LEFT JOIN
             |MAP_IDM.IDM_MAINTAIN_PERSON_RESP_DTL RSP ON
             |RSP.REQUEST_ID = IDM.REQUEST_ID
             |LEFT JOIN (
             |SELECT
             |REQUEST_ID,
             |MEDIA_ID,
             |MAX(CASE WHEN SP_PREF_CD = 'IBM' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_IBM,
             |MAX(CASE WHEN SP_PREF_CD = '10A00' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_10A00,
             |MAX(CASE WHEN SP_PREF_CD = '10G00' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_10G00,
             |MAX(CASE WHEN SP_PREF_CD = '10L00' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_10L00,
             |MAX(CASE WHEN SP_PREF_CD = '10M00' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_10M00,
             |MAX(CASE WHEN SP_PREF_CD = '10N00' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_10N00,
             |MAX(CASE WHEN SP_PREF_CD = '153QH' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_153QH,
             |MAX(CASE WHEN SP_PREF_CD = '15CLV' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15CLV,
             |MAX(CASE WHEN SP_PREF_CD = '15IGO' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15IGO,
             |MAX(CASE WHEN SP_PREF_CD = '15ITT' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15ITT,
             |MAX(CASE WHEN SP_PREF_CD = '15MFT' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15MFT,
             |MAX(CASE WHEN SP_PREF_CD = '15STT' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15STT,
             |MAX(CASE WHEN SP_PREF_CD = '15WCP' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15WCP,
             |MAX(CASE WHEN SP_PREF_CD = '15WSC' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15WSC,
             |MAX(CASE WHEN SP_PREF_CD = '17AAL' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_17AAL,
             |MAX(CASE WHEN SP_PREF_CD = '17BCH' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_17BCH,
             |MAX(CASE WHEN SP_PREF_CD = '17CPH' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_17CPH,
             |MAX(CASE WHEN SP_PREF_CD = '17DSR' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_17DSR,
             |MAX(CASE WHEN SP_PREF_CD = '17ENL' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_17ENL,
             |MAX(CASE WHEN SP_PREF_CD = '17YNI' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_17YNI,
             |MAX(CASE WHEN SP_PREF_CD = '15S8X' THEN SP_TYPE ELSE NULL END) AS PREF_CODE_15S8X
             |FROM
             |MAP_IDM.IDM_MAINTAIN_PERSON_RESP_PREF
             |WHERE
             |REQUEST_ID IN (
             |SELECT
             |REQUEST_ID
             |FROM
             |MAP_IDM.IDM_MAINTAIN_PERSON)
             |GROUP BY
             |REQUEST_ID,
             |MEDIA_ID) AS PRF ON
             |PRF.REQUEST_ID = RSP.REQUEST_ID
             |AND PRF.MEDIA_ID = IMI.EMAIL_MEDIA_ID
             |INNER JOIN (
             |SELECT
             |MAX(CASE WHEN ACTIVITY_NAME = 'Event Interaction' THEN ACTIVITY_TYPE_ID ELSE -1 END ) EVENT_INTERACTION_TYPE_ID,
             |MAX(CASE WHEN ACTIVITY_NAME = 'Contact Interaction' THEN ACTIVITY_TYPE_ID ELSE -1 END ) CONTACT_INTERACTION_TYPE_ID,
             |MAX(CASE WHEN ACTIVITY_NAME = 'Client Interest' THEN ACTIVITY_TYPE_ID ELSE -1 END ) CLIENT_INTEREST_TYPE_ID,
             |MAX(CASE WHEN ACTIVITY_NAME = 'Digital Interaction' THEN ACTIVITY_TYPE_ID ELSE -1 END ) DIGITAL_INTERACTION_TYPE_ID
             |FROM
             |MAP_MKTO.MCT_MKTO_ACTIVITY_TYPE
             |WHERE
             |ACTIVITY_NAME IN ('Event Interaction', 'Contact Interaction', 'Client Interest', 'Digital Interaction')) ACT_ID
             |ON 1 = 1
             |LEFT OUTER JOIN MAP_CORE.MCT_COUNTRY MC ON IMI.PERSON_CTRY_CD = MC.COUNTRY_CODE
             |LEFT OUTER JOIN MAP_IDM.IDM_MAINTAIN_PERSON_RESP_PREF RP_PHONE1 ON
             |RSP.REQUEST_ID = RP_PHONE1.REQUEST_ID
             |AND RSP.PHONE_1_PCMIDPK = RP_PHONE1.MEDIA_ID
             |AND RP_PHONE1.SP_PREF_CD = 'IBM'
             |LEFT OUTER JOIN (
             |SELECT
             |mqap.INBOUND_MKTG_ID,
             |MAX(CASE WHEN mqap.QUESTION_CODE = 'Q_HELP' THEN mqap.ANSWER ELSE NULL END) AS Q_HELP,
             |MAX(CASE WHEN mqap.QUESTION_CODE = 'Q_STUDENT' THEN mqap.ANSWER ELSE NULL END) AS Q_STUDENT,
             |MAX(CASE WHEN mqap.QUESTION_CODE = 'Q_QRADAR_BP' THEN mqap.ANSWER ELSE NULL END) AS Ans_Q_QRADAR_BP,
             |MAX(CASE WHEN mqap.QUESTION_CODE = 'Q_MAIL_CONSENT' THEN mqap.ANSWER ELSE NULL END) AS Ans_Q_MAIL_CONSENT,
             |MAX(CASE WHEN mqap.QUESTION_CODE = 'Q_QRDATA' THEN mqap.ANSWER ELSE NULL END) AS Ans_Q_QRDATA
             |FROM MAP_CORE.MCT_QUESTION_ANSWER_PAIRS mqap GROUP BY INBOUND_MKTG_ID ORDER BY 1) QA_PAIRS
             |ON imi.INBOUND_MKTG_ID = QA_PAIRS.INBOUND_MKTG_ID
             |LEFT OUTER JOIN MAP_CORE.MCT_COP_XREF COP
             |ON COP.IDM_COMPANY_ID = IMI.IDM_COMPANY_ID AND IMI.IDM_COMPANY_ID  > 0
             |WHERE
             |IMI.READY_FOR_MKTO_FLG = 'R'
             |AND IMI.MARKETING_INTERACTION_TYPE_CD = 'MAIL'
             |AND IMI.EMAIL_MEDIA_ID != -1
             |AND MC.SUB_REGION_CODE NOT IN ('4B','4G','4I','4L','4P','4X','4A','4O','4J')
             |),
             |CTE2 AS (
             |SELECT
             |CTE1.INBOUND_MKTG_ID,
             |CTE1.IDM_ID,
             |RANK() OVER(ORDER BY CTE1.CREATE_TS ASC) ranking,
             |CTE1.EMAIL_MEDIA_ID AS EMAIL_MEDIA_ID,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.NEW_TO_IDM_IND
             |ELSE NULL
             |END AS NEW_TO_IDM_IND,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.FIRST_NAME
             |ELSE NULL
             |END AS FIRST_NAME,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.LAST_NAME
             |ELSE NULL
             |END AS LAST_NAME,
             |CTE1.EMAIL_ADDR,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.COMPANY_NAME
             |ELSE NULL
             |END AS COMPANY_NAME,
             |CTE1.CTRY_CODE,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.IDM_COMPANY_ID
             |ELSE NULL
             |END AS IDM_COMPANY_ID,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.DQ_EMAIL_IND
             |ELSE NULL
             |END AS DQ_EMAIL_IND,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.DQ_NAME_IND
             |ELSE NULL
             |END AS DQ_NAME_IND,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.DQ_PHONE_IND
             |ELSE NULL
             |END AS DQ_PHONE_IND,
             |CTE1.CREATE_TS,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.IDM_FEDGOV_IND
             |ELSE NULL
             |END AS IDM_FEDGOV_IND,
             |CTE1.PREF_CODE_IBM,
             |CTE1.COMPANY_PHONE_SUPR_CODE,
             |CTE1.COMPANY_EMAIL_SUPR_CODE,
             |CTE1.PREF_CODE_17YNI,
             |CTE1.PREF_CODE_17ENL,
             |CTE1.PREF_CODE_17DSR,
             |CTE1.PREF_CODE_17CPH,
             |CTE1.PREF_CODE_17BCH,
             |CTE1.PREF_CODE_17AAL,
             |CTE1.PREF_CODE_15WSC,
             |CTE1.PREF_CODE_15WCP,
             |CTE1.PREF_CODE_15STT,
             |CTE1.PREF_CODE_15MFT,
             |CTE1.PREF_CODE_15ITT,
             |CTE1.PREF_CODE_15IGO,
             |CTE1.PREF_CODE_15CLV,
             |CTE1.PREF_CODE_153QH,
             |CTE1.PREF_CODE_10N00,
             |CTE1.PREF_CODE_10M00,
             |CTE1.PREF_CODE_10L00,
             |CTE1.PREF_CODE_10G00,
             |CTE1.PREF_CODE_10A00,
             |CTE1.PREF_CODE_15S8X,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.STUDENT_FLG
             |ELSE NULL
             |END AS STUDENT_FLG,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.IBMER_FLG
             |ELSE NULL
             |END AS IBMER_FLG,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.STATE_CD
             |ELSE NULL
             |END AS STATE_CD,
             |CTE1.WORK_PHONE_PERM,
             |CASE
             |WHEN XREF.EMAIL_MEDIA_ID IS NULL
             |THEN CTE1.SAP_CUST_NUM
             |ELSE NULL
             |END AS SAP_CUST_NUM,
             |CTE1.STATUS_CODE,
             |CTE1.ERROR_CODE,
             |CTE1.ERROR_DESC,
             |CTE1.MIP_TRANS_SRC,
             |CTE1.MIP_TRANS_ID,
             |CTE1.MKTO_PARTITION_ID,
             |CTE1.CAMPAIGN_CODE,
             |CTE1.MKTO_ACTIVITY_ID,
             |CTE1.COUNTRY_CODE,
             |CTE1.ACTIVITY_TYPE_ID,
             |CTE1.LANG_CODE,
             |CTE1.ACTIVITY_NAME,
             |CTE1.ASSET_TYPE,
             |CTE1.ACTIVITY_URL,
             |CTE1.UUC_ID,
             |CTE1.DRIVER_CAMPAIGN_CODE,
             |CTE1.FORM_NAME,
             |CTE1.INTERACTION_ID,
             |CTE1.INTERACTION_TS,
             |CTE1.INTERACTION_TYPE_CODE,
             |CTE1.SCORE_TS,
             |CTE1.STRENGTH,
             |CTE1.UT10_CODE,
             |CTE1.UT15_CODE,
             |CTE1.UT17_CODE,
             |CTE1.UT20_CODE,
             |CTE1.UT30_CODE,
             |CTE1.NEXT_COMM_METHOD_NAME,
             |CTE1.NEXT_COMM_TS,
             |CTE1.NEXT_KEYWORDS,
             |CTE1.NEXT_UUC_ID,
             |CTE1.WEB_PAGE_SRC,
             |CTE1.ACTIVITY_TS,
             |CTE1.IBM_GBL_IOT_CODE,
             |CTE1.SUB_REGION_CODE,
             |CTE1.CAMPAIGN_NAME,
             |CTE1.LEAD_DESC,
             |CTE1.LEAD_NOTE,
             |CTE1.LEAD_SRC_NAME,
             |CTE1.SALES_CHANNEL_NAME,
             |CTE1.CONTACT_PHONE,
             |CTE1.REGION,
             |CTE1.SUB_SRC_DESC,
             |CTE1.EVENT_REF_ID,
             |CTE1.REFERRER_URL,
             |CTE1.ACTIVITY_CMPN_CD,
             |MAXSEQID.MAXSEQID AS MAXSEQID,
             |ACTSEQ.MAXACTSEQID AS MAXACTSEQID
             |FROM CTE1
             |LEFT JOIN (SELECT NVL(MAX(MIP_SEQ_ID), $hrmSequenceNumber) AS MAXSEQID FROM MAP_MKTO.MCT_MKTO_PERSON
             |WHERE MIP_SEQ_ID > '$hrmSequenceNumber') AS MAXSEQID ON 1 = 1
             |LEFT JOIN (SELECT EMAIL_MEDIA_ID FROM MAP_MKTO.MCT_MKTO_LEAD_XREF GROUP BY EMAIL_MEDIA_ID) AS XREF
             |ON XREF.EMAIL_MEDIA_ID = CTE1.EMAIL_MEDIA_ID
             |LEFT JOIN (SELECT NVL(MAX(MIP_ACTIVITY_SEQ_ID), $hrmSequenceNumber) AS MAXACTSEQID FROM MAP_MKTO.MCT_MKTO_CUSTOM_ACTIVITY
             |WHERE MIP_ACTIVITY_SEQ_ID > '$hrmSequenceNumber') AS ACTSEQ ON 1 = 1
             |)
             |SELECT *,
             |MAXSEQID + ROW_NUMBER () OVER (ORDER BY CTE2.CREATE_TS ASC) AS MIP_SEQ_ID,
             |MAXACTSEQID + ROW_NUMBER () OVER (ORDER BY CTE2.CREATE_TS ASC) AS MIP_ACTIVITY_SEQ_ID
             |FROM CTE2
             |FETCH FIRST $maxThreshold ROWS ONLY)""".stripMargin
             println(combinedSqlQuery)

        // MIP Database (IMI & PERSON Table) Connection Details from ETLDataSources Table
        val conProp1: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
        val dbConnectionInfo = conProp1.getProperty(PropertyNames.EndPoint)
        dbCon = DriverManager.getConnection(dbConnectionInfo, conProp1)

        // Creating Combined DataFrame from the above Query
        val combinedDF = AppProperties.SparkSession.read.jdbc(conProp1.getProperty(PropertyNames.EndPoint), combinedSqlQuery, conProp1)
          .dropDuplicates("EMAIL_MEDIA_ID").persist()
        combinedDF.show(false)

        // Creating PersonDF from CombinedDF
        val personTableDF = combinedDF.select("MIP_SEQ_ID",
          "IDM_ID",
          "EMAIL_MEDIA_ID",
          "NEW_TO_IDM_IND",
          "FIRST_NAME",
          "LAST_NAME",
          "EMAIL_ADDR",
          "COMPANY_NAME",
          "CTRY_CODE",
          "IDM_COMPANY_ID",
          "DQ_EMAIL_IND",
          "DQ_NAME_IND",
          "DQ_PHONE_IND",
          "IDM_FEDGOV_IND",
          "STATUS_CODE",
          "ERROR_CODE",
          "ERROR_DESC",
          "MIP_TRANS_SRC",
          "MIP_TRANS_ID",
          "MKTO_PARTITION_ID",
          "CREATE_TS",
          "COMPANY_EMAIL_SUPR_CODE",
          "COMPANY_PHONE_SUPR_CODE",
          "PREF_CODE_IBM",
          "PREF_CODE_10A00",
          "PREF_CODE_10G00",
          "PREF_CODE_10L00",
          "PREF_CODE_10M00",
          "PREF_CODE_10N00",
          "PREF_CODE_153QH",
          "PREF_CODE_15CLV",
          "PREF_CODE_15IGO",
          "PREF_CODE_15ITT",
          "PREF_CODE_15MFT",
          "PREF_CODE_15STT",
          "PREF_CODE_15WCP",
          "PREF_CODE_15WSC",
          "PREF_CODE_17AAL",
          "PREF_CODE_17BCH",
          "PREF_CODE_17CPH",
          "PREF_CODE_17DSR",
          "PREF_CODE_17ENL",
          "PREF_CODE_17YNI",
          "PREF_CODE_15S8X",
          "STUDENT_FLG",
          "IBMer_FLG",
          "STATE_CD",
          "WORK_PHONE_PERM",
          "SAP_CUST_NUM").where("IDM_ID IS NOT NULL AND EMAIL_MEDIA_ID IS NOT NULL AND EMAIL_ADDR IS NOT NULL").dropDuplicates()

        // Creating Custom Activity Table Dataset from Combined DF
        val customActivityTableDF = combinedDF.select("MIP_ACTIVITY_SEQ_ID",
          "MIP_SEQ_ID",
          "ACTIVITY_TYPE_ID",
          "CAMPAIGN_CODE",
          "ACTIVITY_TS",
          "CAMPAIGN_NAME",
          "COUNTRY_CODE",
          "LANG_CODE",
          "ACTIVITY_NAME",
          "ASSET_TYPE",
          "ACTIVITY_URL",
          "UUC_ID",
          "DRIVER_CAMPAIGN_CODE",
          "FORM_NAME",
          "INTERACTION_ID",
          "INTERACTION_TS",
          "SUB_SRC_DESC",
          "LEAD_DESC",
          "LEAD_NOTE",
          "LEAD_SRC_NAME",
          "INTERACTION_TYPE_CODE",
          "CONTACT_PHONE",
          "SALES_CHANNEL_NAME",
          "SCORE_TS",
          "STRENGTH",
          "UT10_CODE",
          "UT15_CODE",
          "UT17_CODE",
          "UT20_CODE",
          "UT30_CODE",
          "NEXT_COMM_METHOD_NAME",
          "NEXT_COMM_TS",
          "NEXT_KEYWORDS",
          "NEXT_UUC_ID",
          "WEB_PAGE_SRC",
          "STATUS_CODE",
          "ERROR_CODE",
          "ERROR_DESC",
          "EVENT_REF_ID",
          "MKTO_ACTIVITY_ID",
          "CREATE_TS",
          "REFERRER_URL",
          "ACTIVITY_CMPN_CD",
          "IBM_GBL_IOT_CODE",
          "SUB_REGION_CODE",
          "REGION",
          "CTRY_CODE",
          "WORK_PHONE_PERM").where("STRENGTH IS NOT NULL AND CAMPAIGN_CODE !='' AND CAMPAIGN_CODE IS NOT NULL")
        customActivityTableDF.show(false)

        // Checking if any Data is Returned
        count = personTableDF.count()
        var counter = count
        if (count > 0) {
          log.info("Reading unprocessed data")
          personTableDF.show(false)

          //Get Column Mapping configuration from ETL_DATA_SOURCE
          val appProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, configSourcePerson)

          //Mapping which will be used to map MIP to MARKETO
          val personColumnMapping = appProp.getProperty(PropertyNames.ResourceSpecific_2).stripMargin.replaceAll("\\s+", "")
          log.info("Creating the column mapping")

          //IMI to MARKETO Mapping
          val personMapDfToMarketoColumns: mutable.Map[String, String] = mutable.Map[String, String]()

          //MARKETO to PERSON Table Mapping
          val personMapDfToDbColumns: mutable.Map[String, String] = mutable.Map[String, String]()

          for (curVal <- personColumnMapping.split(",")) {
            val arrVal = curVal.split("=")
            personMapDfToMarketoColumns += arrVal(0) -> arrVal(1)
            personMapDfToDbColumns += arrVal(1) -> arrVal(0)
          }
          println(personMapDfToMarketoColumns)

          //Creating a List of Target Columns
          var newPersonDF = personTableDF
          val newCollist = personMapDfToMarketoColumns.keys.toList

          //Renaming the Columns to match the Source (IMI) to Target (Marketo)
          for (i <- newCollist) {
            newPersonDF = newPersonDF.withColumnRenamed(i, personMapDfToMarketoColumns(i))
          }

          log.info("Trimming the columns")
          val trimColumns = newPersonDF.schema.fields.filter(_.dataType.isInstanceOf[StringType])
          trimColumns.foreach(f => {
            newPersonDF = newPersonDF.withColumn(f.name, trim(col(f.name)))
            newPersonDF = newPersonDF.withColumn(f.name, regexp_replace(col(f.name), "\\\\", ""))
            newPersonDF = newPersonDF.withColumn(f.name, regexp_replace(col(f.name), "[\\,]", ""))
            newPersonDF = newPersonDF.withColumn(f.name, regexp_replace(col(f.name), "[\\p{C}]", ""))
          })
          log.info("newPersonDF")
          newPersonDF.show(false)

          val spark = AppProperties.SparkSession
          import spark.implicits._

          log.info("Preparing for POST")
          //personMarketoDF is DataFrame used to Update IMI Table
          var personMarketoDF = newPersonDF.persist()

          //personMarketoDF is DataFrame used to Insert into MARKETO
          log.info("Creating the dataframes for the payload")
          personMarketoDF = personMarketoDF
            .select(personMapDfToMarketoColumns.values.toList.distinct.head, personMapDfToMarketoColumns.values.toList.distinct.tail: _*)
          personMarketoDF = personMarketoDF.drop("STATUS_CODE", "ERROR_CODE", "ERROR_DESC", "MKTO_LEAD_ID",
            "MIP_TRANS_SRC", "MIP_TRANS_ID", "MKTO_PARTITION_ID", "CREATE_TS", "CI_Phone_Permission")
          personMarketoDF = personMarketoDF.dropDuplicates(lookupFieldPerson)
          personMarketoDF.show(false)

          //Collecting the MIP_SEQ_ID's posting to Marketo
          mip_seq_id = personMarketoDF.select("MIP_Person_Seq_ID").as[mipSeqId].collect()

          log.info("Creating JSON Payload to be sent to marketo")
          val payload = buildPayloadPerson(personMarketoDF, action, lookupFieldPerson)
          val jsValue = Json.parse(payload)
          println("Payload:" + jsValue)
          Json.prettyPrint(jsValue)

          //Sends post to marketo
          val response = sendPostPerson(payload)

          //Parsing the response to a Dataframe
          log.info("Parsing the response from Marketo")
          val parsedPersonJson = AppProperties.SparkSession.read
            .json(AppProperties.SparkSession.sparkContext.parallelize(Seq(response)).toDS())
          parsedPersonJson.show(false)

          //Create a DataFrame for the Response and Extract LEAD_ID
          val personResponseDF = parsedPersonJson.select(explode(col("result")).as("result")).select("result.*")
          personResponseDF.show(false)

          def hasColumnPerson(df: DataFrame, path: String) = Try(df(path)).isSuccess

          if (hasColumnPerson(personResponseDF, "id")) {
            personFlag = 2
            //Generate the dataframe to update to the source table
            lead_id = personResponseDF.select("id").as[leadId].collect()

            //Capture Error Code & Description for failed records
            if (hasColumnPerson(personResponseDF, "reasons")) {
              val errorDetailsPerson = personResponseDF.where("reasons is not null")
                .select(explode(col("reasons")).as("reasons")).select("reasons.*")
              errorCodePerson = errorDetailsPerson.head().getString(0)
              errorDescPerson = errorDetailsPerson.head().getString(1).replace("'", "")
            }

            log.info("Creating the mapping for Lead ID and MIP_Seq_ID")
            for (i <- mip_seq_id.indices) {
              val leadId: Long = if (lead_id(i).id == null) {
                counter = counter - 1
                errorCounter = "-" + counter.toString
                errorCounter.toLong
              } else {
                lead_id(i).id.toLong
              }
              val mipSeqIdList = mip_seq_id(i).MIP_Person_Seq_ID
              mapPersonIds += leadId -> mipSeqIdList
            }
            println(mapPersonIds)

            val mipSeqIDList = mapPersonIds.values.toString().substring(7, mapPersonIds.values.toString().length)
            val mipSeqIDLeadIdPersonDF = mapPersonIds.toSeq.toDF("leadId", "mipSeqID")
            mipSeqIDLeadIdPersonDF.show(false)

            // Creating Custom Activity Dataframe containing only successful Mip_Seq_Id records that are sent to Marketo-Person.
            var marketoSuccessMipSeqIDList = mipSeqIDLeadIdPersonDF
              .select("mipSeqID", "leadId").where("leadId not like '-%'").distinct()
            personSuccessCount = marketoSuccessMipSeqIDList.count()

            val mipSeqIdSuccessDF = customActivityTableDF
              .join(marketoSuccessMipSeqIDList, customActivityTableDF("MIP_SEQ_ID") === marketoSuccessMipSeqIDList("mipSeqID"), "inner")
              .withColumnRenamed("leadId", "MKTO_LEAD_ID")
            log.info("Successful mipSeqIDs Sent to Marketo")
            mipSeqIdSuccessDF.show(false)

            customCount = mipSeqIdSuccessDF.count()
            if (customCount > 0) {
              log.info("Reading unprocessed data and removing unwanted characters")
              //Get configuration from ETL_DATA_SOURCE
              val appProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, configSourceInteraction)

              //Mapping which will be used to map marketo to DB
              val customActivityColumnMapping = appProp.getProperty(PropertyNames.ResourceSpecific_2).stripMargin.replaceAll("\\s+", "")
              log.info("Creating the column mapping")
              val mapInteractionDfToMarketoColumns: mutable.Map[String, String] = mutable.Map[String, String]()
              val mapInteractionMarketoToDbColumns: mutable.Map[String, String] = mutable.Map[String, String]()
              for (curVal <- customActivityColumnMapping.split(",")) {
                val arrVal = curVal.split("=")
                mapInteractionDfToMarketoColumns += arrVal(0) -> arrVal(1)
                mapInteractionMarketoToDbColumns += arrVal(1) -> arrVal(0)
              }

              var customActivityMarketoDF = mipSeqIdSuccessDF.drop("MIP_SEQ_ID", "SCORE_TS", "NEXT_COMM_METHOD_NAME",
                "NEXT_COMM_TS", "NEXT_KEYWORDS", "NEXT_UUC_ID", "STATUS_CODE", "ERROR_CODE", "ERROR_DESC", "MKTO_ACTIVITY_ID")
              log.info("Dropping Columns Before sending the DF to Marketo")
              customActivityMarketoDF.show(false)

              customActivityMarketoDF = mipSeqIdSuccessDF
                .select(mapInteractionDfToMarketoColumns.keys.toList.distinct.head, mapInteractionDfToMarketoColumns.keys.toList.distinct.tail: _*)
              val marketoInteractionCollist = mapInteractionDfToMarketoColumns.keys.toList

              for (i <- marketoInteractionCollist) {
                customActivityMarketoDF = customActivityMarketoDF.withColumnRenamed(i, mapInteractionDfToMarketoColumns(i))
              }

              log.info("Trimming the columns")
              val trimColumns = customActivityMarketoDF.schema.fields.filter(_.dataType.isInstanceOf[StringType])
              trimColumns.foreach(f => {
                println(f.name)
                customActivityMarketoDF = customActivityMarketoDF.withColumn(f.name, trim(col(f.name)))
                customActivityMarketoDF = customActivityMarketoDF.withColumn(f.name, regexp_replace(col(f.name), "\\\\", ""))
                customActivityMarketoDF = customActivityMarketoDF.withColumn(f.name, regexp_replace(col(f.name), "[\\,]", ""))
                customActivityMarketoDF = customActivityMarketoDF.withColumn(f.name, regexp_replace(col(f.name), "[\\p{C}]", ""))
              })
              customActivityMarketoDF.show(false)

              mip_act_seq_id = customActivityMarketoDF.select("MIP_ACTIVITY_SEQ_ID").as[mipActSeqId].collect()
              println(mip_act_seq_id.mkString("Array(", ", ", ")"))
              log.info("Data to be sent to Marketo")

              log.info("Building payload to be sent to Marketo")
              val interactionPayloadDF = buildPayloadInteraction(customActivityMarketoDF)
              val interactionData = interactionPayloadDF.rdd.map(row => row.getString(0)).collect
              val inputData: String = interactionData.mkString(",")

              val finalInteractionPayload =
                s"""{
               "input": [ $inputData ]
               }""".stripMargin

              log.info("Payload to be sent to Marketo")
              log.info(finalInteractionPayload)

              for (i <- mip_act_seq_id.indices) {
                val interactionMipActSeqIds = mip_act_seq_id(i).MIP_ACTIVITY_SEQ_ID
                interIds += inputData.indexOf("\"" + interactionMipActSeqIds + "\"") -> interactionMipActSeqIds
              }

              val sortedValues = interIds.keys.toArray.sorted
              for (i <- sortedValues) {
                sortedmipActSeqId += interIds.get(i)
              }

              var response = sendPostInteraction(finalInteractionPayload)
              if (response.contains("Access token expired") || response.contains("Access token invalid")) {
                val newToken = getMarketoTokenInteraction
                response = sendPostWithTokenInteraction(finalInteractionPayload, newToken)
              }

              //Parsing the response to a Dataframe to extract Marketo guids
              log.info("Parsing the response from Marketo")
              val parsedInteractionJson = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(response)).toDS())
              parsedInteractionJson.show(false)

              val interactionMarketoResponseDF = parsedInteractionJson.select(explode(col("result"))
                .as("result")).select("result.*")
              interactionMarketoResponseDF.show(false)

              //CHECK IF ANY COLUMN IN RESPONSE DATAFRAME HAS AN ERROR
              def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

              if (hasColumn(interactionMarketoResponseDF, "errors")) {
                val errorDetailsCA = interactionMarketoResponseDF.select(explode(col("errors")).as("errors")).select("errors.*")
                errorCodeCA = errorDetailsCA.head().getString(0)
                errorDescCA = errorDetailsCA.head().getString(1).replace("'", "")
                interactionFlag = 1
              }
              else {
                interactionFlag = 2
              }

              if (interactionFlag == 2) {
                //Generate the dataframe to update to the source table
                marketoID = interactionMarketoResponseDF.select("id").as[guID].collect()
                customSuccessCount = interactionMarketoResponseDF.select("id").count()

                log.info("Creating the mapping for MipActivitySeqId and MarketoGuid")
                for (i <- sortedmipActSeqId.indices) {
                  val mktId = marketoID(i).id
                  val mipActSeqIDFinal = sortedmipActSeqId(i).get
                  mapInteractionIds += mktId -> mipActSeqIDFinal
                }
              }
              else {
                log.info("No HRM's Present To Process for Custom Activity")
              }
            }

            //INSERT INTO PERSON QUEUEING TABLE INCLUDING LEAD_IDs
            insertPerson(personTableDF, mipSeqIDLeadIdPersonDF, dbCon, tgtTableNameP, personMapDfToDbColumns)

            //UPDATING STATUS_CODE IN PERSON TABLE
            log.info("Updating the Status Codes")
            updatePersonTableStatus(personTableDF, dbCon, tgtTableNameP, errorCodePerson, errorDescPerson)

            //INSERT INTO XREF TABLE
            log.info("Inserting into MAP_MKTO.MCT_MKTO_LEAD_XREF")
            val sqlXref =
              s"""(SELECT MIP_SEQ_ID, MKTO_LEAD_ID, EMAIL_ADDR, IDM_ID, EMAIL_MEDIA_ID, 'N' AS PROCESSED_FLG, CAST(NULL as timestamp) AS PROCESSED_TS,
                 |CURRENT_TIMESTAMP AS TRANSACTION_TS, CURRENT_TIMESTAMP AS CREATE_TS, CURRENT_TIMESTAMP AS UPDATE_TS, CREATE_USER
                 |FROM MAP_MKTO.MCT_MKTO_PERSON
                 |WHERE MIP_SEQ_ID IN $mipSeqIDList AND STATUS_CODE = 'P')""".stripMargin
            val conProp2: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
            val xrefDf = AppProperties.SparkSession.read.jdbc(conProp1.getProperty(PropertyNames.EndPoint), sqlXref, conProp2)
            xrefDf.show(false)

            insertIntoXref(xrefDf, dbCon, tgtXrefTableName)

            // INSERT INTO CUSTOM ACTIVITY QUEUEING TABLE
            val customActivityDbDf = customActivityTableDF.drop("RANKING", "CREATE_TS", "REGION", "IBM_GBL_IOT_CODE",
              "SUB_REGION_CODE", "CTRY_CODE", "MKTO_LEAD_ID","WORK_PHONE_PERM").persist()
            log.info("Records to be inserted")
            customActivityDbDf.show(false)
            insertCustomActivity(customActivityDbDf, mipSeqIDLeadIdPersonDF, dbCon, tgtTableNameCA)

            //UPDATE CUSTOM ACTIVITY QUEUEING TABLE WITH ERROR STATUS CODES
            if (interactionFlag == 1) {
              log.info("Custom Activity Response From Marketo Has ERRORS")
              log.info("Updating the error status in Custom Activity Queueing Table")
              updateCustomActivityErrorStatusCode(customActivityDbDf, dbCon, tgtTableNameCA, 'E', errorCodeCA, errorDescCA)
              errorStatusCA = "Failed to Insert Custom Activity HRM's Into Marketo. Check Payload for Errors"
              bException = true
            }
            //UPDATE CUSTOM ACTIVITY QUEUEING TABLE WITH SUCCESS STATUS CODES
            else if (interactionFlag == 2) {
              log.info("Updating the MarketoGuid's")
              val mipActSeqIdSuccessDF = mapInteractionIds.toSeq.toDF("marketoID", "mipActivitySeqID")
              updateCustomActivitySuccessCodes(mipActSeqIdSuccessDF, dbCon, tgtTableNameCA)
              successStatus = "Inserted & Updated Custom Activity HRM's to Marketo & MIP Queueing Table"
            }
            //UPDATE THE STATUS_CODE IN CUSTOM ACTIVITY TABLE WHEN ALL PERSON RECORDS FAILED
            else {
              log.info("Updating the STATUS_CODE for Unprocessed Records")
              updateCustomActivityErrorStatusCode(customActivityDbDf, dbCon, tgtTableNameCA, 'U', null, null)
            }

            //UPDATE MIP_SEQ_ID, PROCESSED_CODE TO 'P', CURRENT TIMESTAMP AS MKTO_QUEUED_TS & READY_FOR_MKTO_FLG TO 'S' INTO IMI TABLE
            updateImiStatus(combinedDF, dbCon, tgtImiTableName)
          }
          else if (hasColumnPerson(personResponseDF, "reasons")) {
            val errorDetailsPerson = personResponseDF.select(explode(col("reasons")).as("reasons")).select("reasons.*")
            errorCodePerson = errorDetailsPerson.head().getString(0)
            errorDescPerson = errorDetailsPerson.head().getString(1).replace("'", "")
            personFlag = 1

            //PERSON INSERT AND UPDATE STATUS_CODE AS 'E'
            insertPersonErrorStatusCode(personTableDF, dbCon, tgtTableNameP, personMapDfToDbColumns, errorCodePerson, errorDescPerson)

            //CUSTOM ACTIVITY INSERT AND UPDATE STATUS_CODE AS 'U'
            insertCustomActivityErrorStatusCode(customActivityTableDF, dbCon, tgtTableNameCA)

            //UPDATE MIP_SEQ_ID, PROCESSED_CODE TO 'P', CURRENT TIMESTAMP AS MKTO_QUEUED_TS & READY_FOR_MKTO_FLG TO 'S' INTO IMI TABLE
            updateImiStatus(combinedDF, dbCon, tgtImiTableName)
          }
        }
        else {
          log.info("No HRM's To Process For Person & Custom Activity ")
        }
      }
        catch
        {
          case e: Throwable =>
            e.printStackTrace()
            e.getMessage
            e.getCause
            log.error(e.getMessage + " - " + e.getCause)
            exceptionMessage = e.getMessage + " - " + e.getCause
            bException = true
        }
      finally
        {
          personCountFailed = count - personSuccessCount

          errString = "Person HRM's Success Count = " + personSuccessCount + ". Person HRM's Failure Count = " + personCountFailed +
            ". " + errorStatusPerson + ". API Error Code = " + errorCodePerson + ". API Error Desc = " + errorDescPerson +
            ". Custom Activity HRM's Failure Count = " + customCount +  ". " + errorStatusCA + ". API Error Code = " + errorCodeCA +
            ". API Error Desc = " + errorDescCA + ". Exception Message = " + exceptionMessage

          successString = "Total Person Success Count = " + personSuccessCount + ". Person HRM's Failure Count = " + personCountFailed + ". " +
            successStatus + ". Total Custom Activity Success Count = " + customSuccessCount

          if (bException) { // Job failed
            DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobFailed, errString, null, null)
            System.exit(1)
          } else {
            DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobSucceeded, successString, null, null)
            System.exit(0)
          }
          log.info("Closing db connections")
          dbCon.close()
          this.cleanUpFramework(AppProperties.SparkSession)
          log.info(s"Exiting Job => $jobClassName...")
        }
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
      else {
        maxTs = Timestamp.valueOf(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").format(LocalDateTime.now(ZoneOffset.UTC)))
      }
    } finally {
      if (dfResult != null) dfResult.unpersist()
    }
    maxTs
  }

  def insertPerson(dataFrame: DataFrame, leadIdDF: DataFrame, dbConn: Connection, insertTableName: String, mapDfToPersonDbColumns: mutable.Map[String, String]): Unit = {
    var personDF = dataFrame.join(leadIdDF, dataFrame("MIP_SEQ_ID") === leadIdDF("mipSeqID"), "left")
    val personColList = mapDfToPersonDbColumns.keys.toList
    for (i <- personColList) {
      personDF = personDF.withColumnRenamed(i, mapDfToPersonDbColumns(i))
    }
    personDF.show()
    personDF = personDF.drop("mipSeqID").withColumnRenamed("leadId", "MKTO_LEAD_ID")
    personDF = personDF.select("MIP_SEQ_ID", "IDM_ID", "EMAIL_MEDIA_ID", "NEW_TO_IDM_IND", "FIRST_NAME", "LAST_NAME",
      "EMAIL_ADDR", "COMPANY_NAME", "CTRY_CODE", "IDM_COMPANY_ID", "DQ_EMAIL_IND", "DQ_NAME_IND", "DQ_PHONE_IND", "IDM_FEDGOV_IND",
      "STATUS_CODE", "ERROR_CODE", "ERROR_DESC", "MKTO_LEAD_ID", "MIP_TRANS_SRC", "MIP_TRANS_ID", "MKTO_PARTITION_ID",
      "COMPANY_EMAIL_SUPR_CODE", "COMPANY_PHONE_SUPR_CODE", "PREF_CODE_IBM", "PREF_CODE_10A00", "PREF_CODE_10G00", "PREF_CODE_10L00",
      "PREF_CODE_10M00", "PREF_CODE_10N00", "PREF_CODE_153QH", "PREF_CODE_15CLV", "PREF_CODE_15IGO", "PREF_CODE_15ITT",
      "PREF_CODE_15MFT", "PREF_CODE_15STT", "PREF_CODE_15WCP", "PREF_CODE_15WSC", "PREF_CODE_17AAL", "PREF_CODE_17BCH",
      "PREF_CODE_17CPH", "PREF_CODE_17DSR", "PREF_CODE_17ENL", "PREF_CODE_17YNI", "PREF_CODE_15S8X","STUDENT_FLG", "IBMer_FLG", "STATE_CD", "WORK_PHONE_PERM",
      "SAP_CUST_NUM")
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      personDF,
      s"""INSERT INTO $insertTableName (MIP_SEQ_ID,IDM_ID,EMAIL_MEDIA_ID,NEW_TO_IDM_IND,FIRST_NAME,LAST_NAME,EMAIL_ADDR,COMPANY_NAME,
         |CTRY_CODE,IDM_COMPANY_ID,DQ_EMAIL_IND,DQ_NAME_IND,DQ_PHONE_IND,IDM_FEDGOV_IND,STATUS_CODE,ERROR_CODE,ERROR_DESC,MKTO_LEAD_ID,
         |MIP_TRANS_SRC,MIP_TRANS_ID,MKTO_PARTITION_ID,COMPANY_EMAIL_SUPR_CODE,COMPANY_PHONE_SUPR_CODE,PREF_CODE_IBM,PREF_CODE_10A00,
         |PREF_CODE_10G00,PREF_CODE_10L00,PREF_CODE_10M00,PREF_CODE_10N00,PREF_CODE_153QH,PREF_CODE_15CLV,PREF_CODE_15IGO,PREF_CODE_15ITT,
         |PREF_CODE_15MFT,PREF_CODE_15STT,PREF_CODE_15WCP,PREF_CODE_15WSC,PREF_CODE_17AAL,PREF_CODE_17BCH,PREF_CODE_17CPH,PREF_CODE_17DSR,
         |PREF_CODE_17ENL,PREF_CODE_17YNI,PREF_CODE_15S8X,STUDENT_FLG,IBMer_FLG,STATE_CD,WORK_PHONE_PERM,SAP_CUST_NUM)
         |VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin,
      personDF.columns,
      Array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48),
      null,true,insertTableName,"INSERT")
  }

  def updatePersonTableStatus(dataFrame: DataFrame, dbConn: Connection, updateTableName: String, errorCodePerson: String, errorDescPerson: String): Unit = {
    val dfUpd = dataFrame.select("MIP_SEQ_ID").toDF()
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET
         |STATUS_CODE = CASE WHEN MKTO_LEAD_ID < 0 THEN 'E' ELSE 'P' END,
         |ERROR_CODE = CASE WHEN MKTO_LEAD_ID < 0 THEN '$errorCodePerson' ELSE NULL END,
         |ERROR_DESC = CASE WHEN MKTO_LEAD_ID < 0 THEN '$errorDescPerson' ELSE NULL END,
         |MKTO_LEAD_ID = CASE WHEN MKTO_LEAD_ID < 0 THEN NULL ELSE MKTO_LEAD_ID END
         |WHERE MIP_SEQ_ID= ?""".stripMargin,
      dfUpd.columns,Array(0),null,true,updateTableName,"UPDATE")
  }

  def insertPersonErrorStatusCode(dataFrame: DataFrame, dbConn: Connection, insertTableName: String, mapDfToPersonDbColumns: mutable.Map[String, String], errorCodePerson: String, errorDescPerson: String): Unit = {
    var personDF = dataFrame
    val personColList = mapDfToPersonDbColumns.keys.toList
    for (i <- personColList) {
      personDF = personDF.withColumnRenamed(i, mapDfToPersonDbColumns(i))
    }
    personDF.show()
    personDF = personDF.drop("CREATE_TS").withColumn("MKTO_LEAD_ID",lit(null))
      .withColumn("STATUS_CODE",lit("E")).withColumn("ERROR_CODE",lit(errorCodePerson))
      .withColumn("ERROR_DESC",lit(errorDescPerson))
    personDF = personDF.select("MIP_SEQ_ID", "IDM_ID", "EMAIL_MEDIA_ID", "NEW_TO_IDM_IND", "FIRST_NAME", "LAST_NAME",
      "EMAIL_ADDR", "COMPANY_NAME", "CTRY_CODE", "IDM_COMPANY_ID", "DQ_EMAIL_IND", "DQ_NAME_IND", "DQ_PHONE_IND", "IDM_FEDGOV_IND",
      "STATUS_CODE", "ERROR_CODE", "ERROR_DESC", "MKTO_LEAD_ID", "MIP_TRANS_SRC", "MIP_TRANS_ID", "MKTO_PARTITION_ID",
      "COMPANY_EMAIL_SUPR_CODE", "COMPANY_PHONE_SUPR_CODE", "PREF_CODE_IBM", "PREF_CODE_10A00", "PREF_CODE_10G00", "PREF_CODE_10L00",
      "PREF_CODE_10M00", "PREF_CODE_10N00", "PREF_CODE_153QH", "PREF_CODE_15CLV", "PREF_CODE_15IGO", "PREF_CODE_15ITT",
      "PREF_CODE_15MFT", "PREF_CODE_15STT", "PREF_CODE_15WCP", "PREF_CODE_15WSC", "PREF_CODE_17AAL", "PREF_CODE_17BCH",
      "PREF_CODE_17CPH", "PREF_CODE_17DSR", "PREF_CODE_17ENL", "PREF_CODE_17YNI", "PREF_CODE_15S8X","STUDENT_FLG", "IBMer_FLG", "STATE_CD",
      "WORK_PHONE_PERM","SAP_CUST_NUM")
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      personDF,
      s"""INSERT INTO $insertTableName (MIP_SEQ_ID,IDM_ID,EMAIL_MEDIA_ID,NEW_TO_IDM_IND,FIRST_NAME,LAST_NAME,EMAIL_ADDR,COMPANY_NAME,
         |CTRY_CODE,IDM_COMPANY_ID,DQ_EMAIL_IND,DQ_NAME_IND,DQ_PHONE_IND,IDM_FEDGOV_IND,STATUS_CODE,ERROR_CODE,ERROR_DESC,MKTO_LEAD_ID,
         |MIP_TRANS_SRC,MIP_TRANS_ID,MKTO_PARTITION_ID,COMPANY_EMAIL_SUPR_CODE,COMPANY_PHONE_SUPR_CODE,PREF_CODE_IBM,PREF_CODE_10A00,
         |PREF_CODE_10G00,PREF_CODE_10L00,PREF_CODE_10M00,PREF_CODE_10N00,PREF_CODE_153QH,PREF_CODE_15CLV,PREF_CODE_15IGO,PREF_CODE_15ITT,
         |PREF_CODE_15MFT,PREF_CODE_15STT,PREF_CODE_15WCP,PREF_CODE_15WSC,PREF_CODE_17AAL,PREF_CODE_17BCH,PREF_CODE_17CPH,PREF_CODE_17DSR,
         |PREF_CODE_17ENL,PREF_CODE_17YNI,PREF_CODE_15S8X,STUDENT_FLG,IBMer_FLG,STATE_CD,WORK_PHONE_PERM,SAP_CUST_NUM)
         |VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin,
      personDF.columns,Array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48),null,true,insertTableName,"INSERT")
  }

  def insertIntoXref(dataFrame: DataFrame, dbConn: Connection, insertTableName: String): Unit = {
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dataFrame,
      s"""INSERT INTO $insertTableName (MIP_SEQ_ID, MKTO_LEAD_ID, EMAIL_ADDR, IDM_ID, EMAIL_MEDIA_ID, PROCESSED_FLG, PROCESSED_TS,
         |TRANSACTION_TS, CREATE_TS, UPDATE_TS, CREATE_USER)
         |VALUES (?,?,?,?,?,?,?,?,?,?,?)""".stripMargin,
      dataFrame.columns,
      Array(0,1,2,3,4,5,6,7,8,9,10),
      null,true,insertTableName,"INSERT")
  }

  def insertCustomActivity(dataFrame: DataFrame, leadIdDF: DataFrame, dbConn: Connection, insertTableName: String): Unit = {
    val dataFrameNew = dataFrame.join(leadIdDF, col("MIP_SEQ_ID") === col("mipSeqID"),"inner")
      .withColumn("STATUS_CODE", expr("CASE WHEN leadId like '-%' THEN 'U' ELSE STATUS_CODE END"))
      .drop("mipSeqID","leadId")

    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dataFrameNew,
      s"""INSERT INTO $insertTableName (MIP_ACTIVITY_SEQ_ID, MIP_SEQ_ID, ACTIVITY_TYPE_ID, CAMPAIGN_CODE, ACTIVITY_TS, CAMPAIGN_NAME,
         |COUNTRY_CODE, LANG_CODE, ACTIVITY_NAME, ASSET_TYPE, ACTIVITY_URL, UUC_ID, DRIVER_CAMPAIGN_CODE, FORM_NAME, INTERACTION_ID,
         |INTERACTION_TS, SUB_SRC_DESC, LEAD_DESC, LEAD_NOTE, LEAD_SRC_NAME, INTERACTION_TYPE_CODE, CONTACT_PHONE, SALES_CHANNEL_NAME,
         |SCORE_TS, STRENGTH, UT10_CODE, UT15_CODE, UT17_CODE, UT20_CODE, UT30_CODE, NEXT_COMM_METHOD_NAME, NEXT_COMM_TS, NEXT_KEYWORDS,
         |NEXT_UUC_ID, WEB_PAGE_SRC, STATUS_CODE, ERROR_CODE, ERROR_DESC, EVENT_REF_ID, MKTO_ACTIVITY_ID, REFERRER_URL, ACTIVITY_CMPN_CD)
         |VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin,
      dataFrameNew.columns,
      Array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41),
      null,true,insertTableName,"INSERT")
  }

  def insertCustomActivityErrorStatusCode(dataFrame: DataFrame, dbConn: Connection, insertTableName: String): Unit = {
    var interactionDF = dataFrame.drop("CREATE_TS", "REGION", "IBM_GBL_IOT_CODE", "SUB_REGION_CODE", "CTRY_CODE")

    interactionDF = interactionDF.withColumn("STATUS_CODE",lit("U"))
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      interactionDF,
      s"""INSERT INTO $insertTableName (MIP_ACTIVITY_SEQ_ID, MIP_SEQ_ID, ACTIVITY_TYPE_ID, CAMPAIGN_CODE, ACTIVITY_TS, CAMPAIGN_NAME,
         |COUNTRY_CODE, LANG_CODE, ACTIVITY_NAME, ASSET_TYPE, ACTIVITY_URL, UUC_ID, DRIVER_CAMPAIGN_CODE, FORM_NAME, INTERACTION_ID,
         |INTERACTION_TS, SUB_SRC_DESC, LEAD_DESC, LEAD_NOTE, LEAD_SRC_NAME, INTERACTION_TYPE_CODE, CONTACT_PHONE, SALES_CHANNEL_NAME,
         |SCORE_TS, STRENGTH, UT10_CODE, UT15_CODE, UT17_CODE, UT20_CODE, UT30_CODE, NEXT_COMM_METHOD_NAME, NEXT_COMM_TS, NEXT_KEYWORDS,
         |NEXT_UUC_ID, WEB_PAGE_SRC, STATUS_CODE, ERROR_CODE, ERROR_DESC, EVENT_REF_ID, MKTO_ACTIVITY_ID, REFERRER_URL, ACTIVITY_CMPN_CD)
         |VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin,
      interactionDF.columns,
      Array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41),
      null,true,insertTableName,"INSERT")
  }

  def updateCustomActivityErrorStatusCode(dataFrame: DataFrame, dbConn: Connection, updateTableName: String, statusCode: Char, errorCode: String, errorDesc: String): Unit = {
    val errorDescNew = errorDesc.replaceAll("\'", "")
    val dfUpd = dataFrame.select("MIP_ACTIVITY_SEQ_ID")
    dfUpd.show(false)
    println(s"""UPDATE $updateTableName SET STATUS_CODE = $statusCode, ERROR_CODE = $errorCode, ERROR_DESC = '$errorDescNew'
               |WHERE MIP_ACTIVITY_SEQ_ID = ?""".stripMargin)
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = '$statusCode', ERROR_CODE = $errorCode, ERROR_DESC = '$errorDescNew'
         |WHERE MIP_ACTIVITY_SEQ_ID = ?""".stripMargin,
      dfUpd.columns,Array(0),null,true,updateTableName,"UPDATE")
  }

  def updateCustomActivitySuccessCodes(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dataFrame,
      s"""UPDATE $updateTableName SET MKTO_ACTIVITY_ID = ?, STATUS_CODE = 'P' WHERE MIP_ACTIVITY_SEQ_ID = ?""",
      dataFrame.columns,Array(0,1),null,true,updateTableName,"UPDATE")
  }

  def updateImiStatus(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {
    val dfUpd = dataFrame.select("MIP_SEQ_ID", "INBOUND_MKTG_ID").toDF()
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET READY_FOR_MKTO_FLG = 'S', PROCESSED_CD = 'P', MKTO_QUEUED_TS = CURRENT_TIMESTAMP, MIP_SEQ_ID = ?
         |WHERE INBOUND_MKTG_ID= ?""".stripMargin,
      dfUpd.columns,Array(0,1),null,true,updateTableName,"UPDATE")
  }
}
