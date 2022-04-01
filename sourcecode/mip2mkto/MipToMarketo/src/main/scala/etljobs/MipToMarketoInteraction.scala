package etljobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.exception.RestServiceException
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.sql.functions.{col, explode, regexp_replace, trim}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{JArray, _}
import play.api.libs.functional.syntax.toFunctionalBuilderOps
import play.api.libs.json.Reads._
import play.api.libs.json.{JsPath, Reads}
import scala.util.Try
import java.net.SocketTimeoutException
import java.sql.{Connection, DriverManager, Statement, Timestamp}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.exit


object MipToMarketoInteraction extends ETLFrameWork {

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
  var payloadInteraction: String = null
  var contactInteraction: Long = 0
  var clientInterest: Long = 0
  var eventInteraction: Long = 0
  var digitalInteraction: Long = 0

  override def getPropertiesFromJson(json: String): Properties = super.getPropertiesFromJson(json)
  var lookupField: String = null
  var finalJoinedDFCount: Long = 0
  var count : Long = 0
  var count2 : Long = 0


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
  def buildPayloadInteraction(transformedDF: DataFrame): DataFrame = {

    val spark = AppProperties.SparkSession
    var activityDF1: DataFrame = null
    var activityDF2: DataFrame = null
    var activityDF3: DataFrame = null
    var activityDF4: DataFrame = null

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
         |
         |                },
         |                {
         |                    "apiName": "Item_URL",
         |                    "value": "${removeControlChar(row.getString(23))}"
         |
         |                }
         |          ]
         |}""".stripMargin

    }).toDF().persist()

    activityDF2 = transformedDF.filter(transformedDF("Activity_Type") === clientInterest).map(row => {
      s"""{
         |      "leadId":${row.getLong(9)},
         |      "activityDate":"${convert({row.getTimestamp(31).toString})}",
         |      "activityTypeId":${row.getInt(19)},
         |      "primaryAttributeValue": "${row.getString(25)}",
         |      "attributes":
         |          [
         |              {
         |                    "apiName": "Item_Name",
         |                    "value": "${removeControlChar(row.getString(0))}"
         |                },
         |                {
         |                    "apiName": "Item_URL",
         |                    "value": "${removeControlChar(row.getString(18))}"
         |                },
         |                {
         |                    "apiName": "Item_ID",
         |                    "value": "${removeControlChar(row.getString(11))}"
         |                },
         |                {
         |                    "apiName": "Content_Campaign_Name",
         |                    "value": "${removeControlChar(row.getString(24))}"
         |                },
         |                {
         |                    "apiName": "CI_TS",
         |                    "value": "${DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").format(LocalDateTime.now)}"
         |
         |                },
         |                {
         |                    "apiName": "GBL_IMT_CD",
         |                    "value": "${removeControlChar(row.getString(7))}"
         |
         |                },
         |                {
         |                    "apiName": "GBL_IOT_CD",
         |                    "value": "${removeControlChar(row.getString(15))}"
         |
         |                },
         |                {
         |                    "apiName": "GBL_RGN_CD",
         |                    "value": "${removeControlChar(row.getString(21))}"
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
         |                    "apiName": "Interaction_ID",
         |                    "value": "${row.getLong(33)}"
         |
         |                },
         |                {
         |                    "apiName": "Sales_Channel",
         |                    "value": "${removeControlChar(row.getString(16))}"
         |
         |                },
         |                {
         |                    "apiName": "Create_TS",
         |                    "value": "${DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").format(LocalDateTime.now)}"
         |                },
         |                {
         |                    "apiName": "MIP_Activity_Seq_ID",
         |                    "value": "${row.getLong(14)}"
         |                },
         |                {
         |                    "apiName": "CI_Phone_Permission",
         |                    "value": "${removeControlChar(row.getString(6))}"
         |                }
         |          ]
         |}""".stripMargin
    }).toDF().persist()

    activityDF3 = transformedDF.filter(transformedDF("Activity_Type") === eventInteraction).map(row => {
      s"""{
         |      "leadId":${row.getLong(9)},
         |      "activityDate":"${convert({row.getTimestamp(31).toString})}",
         |      "activityTypeId":${row.getInt(19)},
         |      "primaryAttributeValue": "${row.getString(22)}",
         |      "attributes":
         |          [
         |              {
         |                    "apiName": "Content_Campaign_Code",
         |                    "value": "${removeControlChar(row.getString(25))}"
         |                },
         |                {
         |                    "apiName": "Interaction_ID",
         |                    "value": "${row.getLong(33)}"
         |                },
         |                {
         |                    "apiName": "Create_TS",
         |                    "value": "${DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").format(LocalDateTime.now)}"
         |                },
         |                {
         |                    "apiName": "Interaction_Type_Code",
         |                    "value": "${removeControlChar(row.getString(20))}"
         |                },
         |                {
         |                    "apiName": "Driver_Campaign_Code",
         |                    "value": "${removeControlChar(row.getString(17))}"
         |
         |                },
         |                {
         |                    "apiName": "MIP_Activity_Seq_ID",
         |                    "value": "${row.getLong(14)}"
         |                },
         |                {
         |                    "apiName": "Activity_Campaign_Code",
         |                    "value": "${removeControlChar(row.getString(29))}"
         |                }
         |          ]
         |}""".stripMargin
    }).toDF().persist()

    activityDF4 = transformedDF.filter(transformedDF("Activity_Type") === digitalInteraction).map(row => {
      s"""{
         |      "leadId":${row.getLong(9)},
         |      "activityDate":"${convert({row.getTimestamp(31).toString})}",
         |      "activityTypeId":${row.getInt(19)},
         |      "primaryAttributeValue": "${row.getString(25)}",
         |      "attributes":
         |          [
         |              {
         |                    "apiName": "Item_Country_Code",
         |                    "value": "${removeControlChar(row.getString(34))}"
         |                },
         |                {
         |                    "apiName": "Item_Language_Code",
         |                    "value": "${removeControlChar(row.getString(12))}"
         |                },
         |                {
         |                    "apiName": "Item_Name",
         |                    "value": "${removeControlChar(row.getString(0))}"
         |                },
         |                {
         |                    "apiName": "Item_Type",
         |                    "value": "${removeControlChar(row.getString(2))}"
         |                },
         |                {
         |                    "apiName": "Item_URL",
         |                    "value": "${removeControlChar(row.getString(18))}"
         |
         |                },
         |                {
         |                    "apiName": "Item_ID",
         |                    "value": "${removeControlChar(row.getString(11))}"
         |
         |                },
         |                {
         |                    "apiName": "Driver_Campaign_Code",
         |                    "value": "${removeControlChar(row.getString(17))}"
         |
         |                },
         |                {
         |                    "apiName": "Form_Name",
         |                    "value": "${removeControlChar(row.getString(35))}"
         |
         |                },
         |                {
         |                    "apiName": "Strength",
         |                    "value": ${row.getLong(1)}
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
         |                    "apiName": "Interaction_ID",
         |                    "value": "${row.getLong(33)}"
         |
         |                },
         |                {
         |                    "apiName": "Create_TS",
         |                    "value": "${DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").format(LocalDateTime.now)}"
         |
         |                },
         |                {
         |                    "apiName": "Interaction_Type_Code",
         |                    "value": "${removeControlChar(row.getString(20))}"
         |
         |                },
         |                {
         |                    "apiName": "Person_Country_Code",
         |                    "value": "${removeControlChar(row.getString(3))}"
         |
         |                },
         |                {
         |                    "apiName": "MIP_Activity_Seq_ID",
         |                    "value": "${row.getLong(14)}"
         |                }
         |          ]
         |}""".stripMargin
    }).toDF().persist()


    val finalDF = Seq(activityDF1, activityDF2, activityDF3, activityDF4)

    finalDF.reduce(_ union _)
  }

  def remove_string: String => String = _.replaceAll("[[\\p{C}]]", "")

  //Function to perform POST to Marketo
  def sendPost(payload: String): String = {

    val apiConProps:Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val externalEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
    var postResponse: String = null
    val httpPostToken = null
    val httpClient: CloseableHttpClient = HttpClients.custom().build()

    //Exception handling for duplicates in Payload
    try {
      val token = getMarketoToken
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

  //Function to perform POST to Marketo
  def sendPostWithToken(payload: String, token: String): String = {

    val apiConProps:Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
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
    contactInteraction = args(args.indexOf("--contactInteraction") + 1).toLong
    clientInterest = args(args.indexOf("--clientInterest") + 1).toLong
    digitalInteraction = args(args.indexOf("--digitalInteraction") + 1).toLong
    eventInteraction = args(args.indexOf("--eventInteraction") + 1).toLong

    log.info("Initialization started")
    this.initializeFramework(args)
    log.info("Initialization completed.")
    log.info(s"Starting ETL Job => $jobClassName....")

    val jobSequence = s"$jobClassName"
    //val jobSequence = "testUTCInt"

    val fetchRows = 300
    val last_run_timestamp = getMaxRecordTimestampTest(jobSequence)
    var mipActSeqId = Array[mipActSeqID]()
    var marketoID = Array[guID]()
    var interID = ArrayBuffer[String]()
    val sortedmipActSeqId = ArrayBuffer[Option[Long]]()
    var flag = 0
    var errorCode:String = null
    var errorDesc:String = null
    var dbCon:Connection = null
    var dbConEvent:Connection = null

    var mapIds = scala.collection.mutable.Map[Option[Long], Long]()
    var interIds = scala.collection.mutable.Map[Int, Long]()

    DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobStarted, "GOOD LUCK", null, null)
    log.info("ETL logic goes here...")

    try {
      val sqlData =
        s"""(SELECT MIP_ACTIVITY_SEQ_ID FROM MAP_MKTO.MCV_MKTO_CUSTOM_ACTIVITY WHERE ACTIVITY_TYPE_ID = 100004 AND EVENT_REF_ID IS NULL AND EVENT_REF_ID = '' AND STATUS_CODE = 'U')""".stripMargin

      val conProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
      val filterEvent = AppProperties.SparkSession.read.jdbc(conProp.getProperty(PropertyNames.EndPoint), sqlData, conProp)
      val updateCount = filterEvent.count()
      log.info("EVENT_REF_ID with null values count " + updateCount)
      val dbConnectionInfoEvent = conProp.getProperty(PropertyNames.EndPoint)
      dbConEvent = DriverManager.getConnection(dbConnectionInfoEvent, conProp)

      if(updateCount > 0)
      {
        updateEventErrorStatusCode(filterEvent,dbConEvent,tgtTableName)
      }

      val sqlData1 =
        s"""(WITH ranked_data AS (
           |SELECT CAMPAIGN_CODE,
           |RANK() OVER(
           |ORDER BY a.CREATE_TS ASC) ranking,
           |MKTO_ACTIVITY_ID, COUNTRY_CODE, ACTIVITY_TYPE_ID, LANG_CODE, ACTIVITY_NAME, ASSET_TYPE, ACTIVITY_URL, UUC_ID, DRIVER_CAMPAIGN_CODE, FORM_NAME, INTERACTION_ID,
           |INTERACTION_TS, INTERACTION_TYPE_CODE, STRENGTH, UT10_CODE, UT15_CODE, UT17_CODE, UT20_CODE, UT30_CODE, WEB_PAGE_SRC, a.MKTO_LEAD_ID, ACTIVITY_TS, CTRY_CODE, IBM_GBL_IOT_CODE, SUB_REGION_CODE, CAMPAIGN_NAME,
           |LEAD_DESC, LEAD_NOTE, LEAD_SRC_NAME, SALES_CHANNEL_NAME, CONTACT_PHONE, REGION, SUB_SRC_DESC, a.CREATE_TS, EVENT_REF_ID, a.MIP_ACTIVITY_SEQ_ID, REFERRER_URL, ACTIVITY_CMPN_CD, a.WORK_PHONE_PERM
           |FROM
           |MAP_MKTO.MCV_MKTO_CUSTOM_ACTIVITY a
           |LEFT JOIN
           |MAP_MKTO.MCT_MKTO_PERSON p on
           |p.MIP_SEQ_ID = a.MIP_SEQ_ID
           |WHERE p.STATUS_CODE = 'P' AND a.STATUS_CODE = 'U' AND a.STRENGTH IS NOT NULL AND CAMPAIGN_CODE !='' AND CAMPAIGN_CODE IS NOT NULL
           |ORDER BY 2 asc),
           |etl_config_data AS (
           |SELECT
           |$minBatchSize record_limit,
           |$elapsedTime elapsed_time_limit_in_mins,
           |'$last_run_timestamp' AS last_sync_timestamp
           |FROM sysibm.sysdummy1)
           |SELECT * FROM
           |ranked_data,
           |etl_config_data
           |WHERE
           |record_limit <= (
           |SELECT
           |count(1)
           |FROM
           |ranked_data)
           |OR timestampdiff( 4, CURRENT timestamp - last_sync_timestamp ) > elapsed_time_limit_in_mins
           |FETCH FIRST $maxThreshold ROWS ONLY)""".stripMargin


      println(sqlData1)
      val conProp1: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbSource)
      val dfResult1 = AppProperties.SparkSession.read.jdbc(conProp1.getProperty(PropertyNames.EndPoint), sqlData1, conProp1)
      count = dfResult1.count()

      println("Count of records:" + count)

      val dbConnectionInfo = conProp1.getProperty(PropertyNames.EndPoint)
      dbCon = DriverManager.getConnection(dbConnectionInfo, conProp1)

      if(count > 0) {
        log.info("Reading unprocessed data and removing unwanted characters")

        //Get configuration from ETL_DATA_SOURCE
        val appProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, configSource)

        //Mapping which will be used to map marketo to DB
        log.info("Creating the column mapping")
        val columnMapping = appProp.getProperty(PropertyNames.ResourceSpecific_2).stripMargin.replaceAll("\\s+", "")
        val mapDfToDBColumns: mutable.Map[String, String] = mutable.Map[String, String]()
        for (curVal <- columnMapping.split(",")) {
          val arrVal = curVal.split("=")
          mapDfToDBColumns += arrVal(0) -> arrVal(1)
        }

        var newDF = dfResult1.select(mapDfToDBColumns.keys.toList.distinct.head, mapDfToDBColumns.keys.toList.distinct.tail: _*)
        val newCollist = mapDfToDBColumns.keys.toList

        for (i <- newCollist) {
          newDF = newDF.withColumnRenamed(i, mapDfToDBColumns(i))
        }

        log.info("Trimming the columns")
        val trimColumns = newDF.schema.fields.filter(_.dataType.isInstanceOf[StringType])
        trimColumns.foreach(f => {
          newDF = newDF.withColumn(f.name, trim(col(f.name)))
          newDF = newDF.withColumn(f.name, regexp_replace(col(f.name), "\"", ""))
          newDF = newDF.withColumn(f.name, regexp_replace(col(f.name), "[\\,]", ""))
          newDF = newDF.withColumn(f.name, regexp_replace(col(f.name), "[\\p{C}]", ""))
        })

        val spark = AppProperties.SparkSession
        import spark.implicits._

        newDF = newDF.persist()
        // Dataframe that shows the numbering used to build the payload
        log.info("Dataframe for numbering used to build the payload")
        newDF.show(false)

        mipActSeqId = newDF.select("MIP_Activity_Seq_ID").as[mipActSeqID].collect()

        println(mipActSeqId.mkString("Array(", ", ", ")"))
        log.info("Data to be sent to Marketo")

        log.info("Building payload to be sent to Marketo")
        val finalDF = buildPayloadInteraction(newDF)

        val testData = finalDF.rdd.map(row => row.getString(0)).collect
        val inputData: String = testData.mkString(",")

        val finalPayload =
          s"""{
               "input": [ $inputData ]
          }""".stripMargin

        log.info("Payload to be sent to Marketo")
        log.info(finalPayload)

        for (i <- mipActSeqId.indices) {
          val emailMediaId = mipActSeqId(i).MIP_ACTIVITY_SEQ_ID
          interIds += inputData.indexOf("\"" + emailMediaId + "\"") -> emailMediaId
        }

        val sortedValues = interIds.keys.toArray.sorted
        for (i <- sortedValues) {
          sortedmipActSeqId += interIds.get(i)
        }

        var response = sendPost(finalPayload)

        if (response.contains("Access token expired") || response.contains("Access token invalid")) {
          val newToken = getMarketoToken
          response = sendPostWithToken(finalPayload, newToken)
        }

        log.info("Parsing the response from Marketo")
        //Parsing the response to a Dataframe to extract lead id
        val parsedJson = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(response)).toDS())
        parsedJson.show(false)

        val testDF2 = parsedJson.select(explode(col("result")).as("result")).select("result.*")
        testDF2.show(false)

        def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

        if (hasColumn(testDF2, "errors")) {
          testDF2.show(false)
          val errorDetails = testDF2.select(explode(col("errors")).as("errors")).select("errors.*")
          errorCode = errorDetails.head().getString(0)
          errorDesc = errorDetails.head().getString(1)
          flag = 1
        }
        else {
          //Generate the dataframe to update to the source table
          marketoID = testDF2.select("id").as[guID].collect()

          log.info("Creating the mapping for MipActivitySeqId and MarketoGuid")
          for (i <- sortedmipActSeqId.indices) {
            val mktId = marketoID(i).id
            val mipActSeqIDFinal = sortedmipActSeqId(i).get
            mapIds += mktId -> mipActSeqIDFinal
          }

          val postDF = mapIds.toSeq.toDF("marketoID", "mipActivitySeqID")
          postDF.show(false)

          log.info("Updating the MarketoGuid's")
          //Function which will update the LeadID for the processed messages
          updatePostStatusId(postDF, dbCon, tgtTableName)

          log.info("Updating the status code's")
          updatePostStatusCode(newDF, dbCon, tgtTableName)
        }
        if (flag == 1) {
          log.info("Updating the error status")
          updateErrorStatusCode(newDF, dbCon, tgtTableName, errorCode, errorDesc)
          bException = true
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
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
    }
    finally
    {
      val errString = "Check Payload. Example for an " + "Error Code: " + errorCode + ". & Error Description: " + errorDesc
      val ErrorCode = "Error Code: " + errorCode + "."
      val ErrorDesc = "Error Description: " + errorDesc + "."
      if (bException) { // Job failed
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobFailed, errString,ErrorCode, ErrorDesc)
        exit(1)
      } else {
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobSucceeded, "Job completed without any error", ErrorCode, ErrorDesc)
      }
      log.info("Closing db connections")
      dbConEvent.close()
      dbCon.close()
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }

  //Case class definition to extract from JSON response
  //case class resultPost(id: String, status: String)
  case class resultPost(no: String, status: String)
  case class resultResponse(requestId: String, result: resultPost, success: String)
  case class mipActSeqID(MIP_ACTIVITY_SEQ_ID: Long)
  case class guID(id: Option[Long] = null)
  case class emailID(interactionID: JArray)
  case class resultErrorPost(code: String, message: String)
  case class resultErrorResponse(requestId: String, result: resultErrorPost, success: String)


  implicit val readPost: Reads[resultPost] = (
    (JsPath \ "no").read[String] and
      (JsPath \ "status").read[String]
    ) (resultPost.apply _)

  implicit val cert: Reads[resultResponse] = (
    (JsPath \ "requestId").read[String] and
      (JsPath \ "result").read[resultPost] and
      (JsPath \ "success").read[String]
    ) (resultResponse.apply _)

  //Updating data status after posting it in marketo
  def updatePostStatusId(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {

    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dataFrame,
      s"""UPDATE $updateTableName SET MKTO_ACTIVITY_ID = ? WHERE MIP_ACTIVITY_SEQ_ID = ?""",
      dataFrame.columns,
      Array(0,1),
      null,
      true,updateTableName,
      "UPDATE")
  }

  def updatePostStatusCode(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {
    val dfUpd = dataFrame.select("MIP_Activity_Seq_ID").toDF()
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = 'P', ERROR_CODE = NULL, ERROR_DESC = NULL WHERE MIP_ACTIVITY_SEQ_ID= ?""",
      dfUpd.columns,
      Array(0),
      null,
      true,updateTableName,
      "UPDATE")
  }

  def updateEventErrorStatusCode(dataFrame: DataFrame, dbConn: Connection, updateTableName: String): Unit = {
    val dfUpd = dataFrame.select("MIP_ACTIVITY_SEQ_ID").toDF()
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = 'E', ERROR_CODE = NULL, ERROR_DESC = 'EVENT BRIEF ID CANNOT BE NULL OR EMPTY' WHERE MIP_ACTIVITY_SEQ_ID= ?""",
      dfUpd.columns,
      Array(0),
      null,
      true,
      updateTableName,
      "UPDATE")
  }

  def updateErrorStatusCode(dataFrame: DataFrame, dbConn: Connection, updateTableName: String, errorCode: String, errorDesc: String): Unit = {
    val errorDescNew = errorDesc.replaceAll("\'", "")
    val dfUpd = dataFrame.select("MIP_Activity_Seq_ID").toDF()
    DataUtilities.runPreparedStatementUsingConnection(
      dbConn,
      dfUpd,
      s"""UPDATE $updateTableName SET STATUS_CODE = 'E', ERROR_CODE = $errorCode, ERROR_DESC = '$errorDescNew' WHERE MIP_ACTIVITY_SEQ_ID= ? AND STATUS_CODE != 'P'""",
      dfUpd.columns,
      Array(0),
      null,
      true, updateTableName,
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

  def convert(ts: String): String = {
    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val date = df1.parse(ts)
    val epoch = date.getTime
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS-05:00")
    df.format(epoch)
  }

  def removeControlChar(ts: String): String = {
    var newVal:String = null
    if(ts == null || ts == ""){
      ts
    }
    else
    {
      newVal = ts.replaceAll("[\\p{C}]", "").replaceAll("[\\r\\n\\t]", "")
      val finalVal = newVal.replaceAll("\"", "")
      finalVal
    }
  }
}