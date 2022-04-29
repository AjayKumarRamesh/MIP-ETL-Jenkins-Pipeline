import com.ibm.mkt.etlframework.audit.JobRunArgs
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.sql.functions.{col, explode}
import org.json4s.jackson.JsonMethods.parse
import java.sql.{Connection, Statement}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

//This job has two main steps
//STEP 1: Call API to connect marketo and retrieve email addresses using LEAD_ID's, load them into src MCT_MKTO_EMAIL_ACTIVITY table on MIP
//STEP 2: Once emails have been populated, extract data from src MCT_MKTO_EMAIL_ACTIVITY table and load into Target IDM MAP_IDM.IDM_MAINTAIN_PRIVPREF_DEE table
object MIP_Unsubs_Email_IDM extends ETLFrameWork {

  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var marketoCreds: ArrayBuffer[String] = ArrayBuffer[String]()
  var apiSource: String = null
  var dbTgtSource: String = null
  var errstr: String = null
  var tgtTableName: String = null
  var processedCount: Long = 0


  case class Attributes(name: String, value: String)

  def main(args: Array[String]): Unit = {

    var isJobFailed: Boolean = false
    args.foreach(println(_))
    val UnsubEmail: String = args(args.indexOf("--tgtTable") + 1)
    // passing as argument to connect MARKETO_API_NEW, we can change it if env changes without changing scala code
    apiSource = args(args.indexOf("--apiSource") + 1)
    dbTgtSource = args(args.indexOf("--dbTgtSource") + 1)
    tgtTableName = args(args.indexOf("--tgtTable") + 1)
    //logging job run statistics into Job history table
    try {
      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")
      log.info(s"Starting ETL Job => $jobClassName....")
      log.info("JOBHISTORYTABLE INSERT STARTED")
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobStarted)
      log.info("JOBHISTORYTABLE INSERT ENDED")
      runJobSequence(UnsubEmail)
      log.info(s"Completed Job => $jobClassName.")

      val jobrun = new JobRunArgs
      jobrun.jobMetrics = "Total Count of Records Processed " + processedCount
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobSucceeded, jobrun)
      log.info(s"Completed Job => $jobClassName.") //logging when job is succeeded

    } catch {
      case e: Throwable => {
        isJobFailed = true
        e.printStackTrace
        log.error(e.getMessage + " - " + e.getCause)
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
  def runJobSequence(UnsubEmail: String): Unit = {

    var leadString = ""
    val conProp: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbTgtSource)
    var dfTest: DataFrame = null
    //building dataframe to get list of Lead_ids and use them in API lead search
    val sqlStmt: String = "SELECT ID, EMAIL,LEAD_ID, MOD(ROW_NUMBER() OVER(ORDER BY LEAD_ID), 4) AS PARTCOL " +
      "FROM MAP_MKTO.MCT_MKTO_EMAIL_ACTIVITY WHERE ACTIVITY_TYPE_ID = 9 AND PROCESSED_FLAG = 'N' and EMAIL is NULL ORDER BY CREATE_TS FETCH FIRST 300 ROWS ONLY"
    //dfTest = DataUtilities.readData(AppProperties.SparkSession, conProp, sqlStmt, null)
    dfTest = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, conProp, sqlStmt, Constants.PartitionTypeByColumn, null, "PARTCOL")


    dfTest.show(false)

    val leadId = dfTest.select(col("LEAD_ID")).collect.flatMap(_.toSeq)
    val emptyEmailId = dfTest.select(col("LEAD_ID")).filter("EMAIL IS NULL").toDF()
    emptyEmailId.show()

    //val dfUpd = dataFrame.select("id", "email")

    for (i <- leadId.indices) {
      leadString += leadId.mkString(",")
    } //creating array of lead_ids to pass as filter type in API

    leadString = leadId.mkString(",")


    var mktCreds: ArrayBuffer[String] = getMarketoToken
    var accessToken = mktCreds(0)
    var response = performGet("id", leadString, accessToken, "email,Unsubscribed")

    // CHECKING IF THE ACCESS TOKEN HAS EXPIRED OR NOT - STARTED
    if (response.contains("Access token expired") || response.contains("Access token invalid")) {
      mktCreds = getMarketoToken
      accessToken = mktCreds(0)
      response = performGet("id", leadString, accessToken, "email,Unsubscribed")
    }
    log.info("response:" + response)
    if (response.contains("result") && !response.contains("[]")) {
      val spark = AppProperties.SparkSession
      import spark.implicits._
      val df = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(response)).toDS())
      df.show(false)
      val newDF = df.select(explode(col("result")).as("result")).select("result.*")
      newDF.show(false)
      updatePostStatus(newDF, conProp)
    }
    //Traverse the result field from the JSON


    else {
      log.info("No response found for processing from Marketo or API call failure")
    }

    //build source dataframe
    var dfFinal = DataUtilities.readDataByPartitioningType(
      AppProperties.SparkSession,
      conProp,
      "SELECT ID,src.ACTIVITY_DATE AS ORIG_TRANS_TS,  COALESCE(src.MAILING_ID,'') AS ORIG_TRANS_ID,'MIP' as SOURCE_PROCESS, " +
        "'P' AS PERSON_ORG_IND,'EML' AS PRIV_PREF_SCOPE,'S' AS SP_VALUE, 'C' AS SP_ORIGIN," +
        "'FT' AS SP_REASON,src.ACTIVITY_DATE AS SP_REQ_TS,src.MARKETO_GUID AS SP_COMMENT,src.EMAIL AS CM_REF_NUM," +
        "'MARKETO' AS DATA_SOURCE,src.LEAD_ID AS SP_TRANS_ID,src.CAMPAIGN_ID AS SP_CMPN  FROM MAP_MKTO.MCT_MKTO_EMAIL_ACTIVITY src " +
        "WHERE ACTIVITY_TYPE_ID = 9 AND PROCESSED_FLAG = 'N' and EMAIL is not null ORDER BY CREATE_TS FETCH FIRST 300 ROWS ONLY",
      Constants.PartitionTypeByRowNumber,
      null,
      null
    )

    log.info(s"${dfFinal.count()} row(s) extracted")
    dfFinal.show(numRows = 10, truncate = false)
    dfFinal = dfFinal.persist()

    val dfTgt = dfFinal.drop(Constants.DefaultPartitionColumnName).drop(colName = "ID")
    processedCount = dfTgt.count()
    if (!dfFinal.isEmpty) {
      println("Writing to DB")
      val connectionProperties: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbTgtSource)
      connectionProperties.put("driver", "com.ibm.db2.jcc.DB2Driver")
      connectionProperties.put("sslConnection", "true")
      dfTgt.write.mode("append")
        .jdbc(connectionProperties.getProperty(PropertyNames.EndPoint), tgtTableName, connectionProperties)
      //Inserting data into IDM Target table
      //      DataUtilities.saveDataframeToDB(AppProperties.SparkSession,
      //        conProp,
      //         dfTgt,
      //        "MAP_IDM.IDM_MAINTAIN_PRIVPREF_DEE",
      //        false,
      //        Constants.SaveModeAppend)
    }
    else {
      log.info("Source Datafrafme is empty")
    }
    log.info("Wrote to MAP_IDM.IDM_MAINTAIN_PRIVPREF_DEE")


    //setting up flag to 'Y' for processed records
    val dfUpd = dfFinal.select("ID")
    DataUtilities.runPreparedStatement(
      conProp,
      dfUpd, "UPDATE MAP_MKTO.MCT_MKTO_EMAIL_ACTIVITY SET PROCESSED_FLAG = 'Y',UPDATE_TS = CURRENT_TIMESTAMP WHERE ID = ?",
      dfUpd.columns,
      Array(0), null, "UPDATE")

  }

  //connecting to Marketo -API
  def getMarketoToken: ArrayBuffer[String] = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource) //Initiating spark session
    val clientID = apiConProps.getProperty(PropertyNames.ClientID) //connects to MARKETO API using MARKETO_CLIENT_ID UserId k8 secret
    val clientSecret = apiConProps.getProperty(PropertyNames.ClientSecret) //connects to MARKETO API using MARKETO_CLIENT_ID PW k8 secret
    val identityEndpoint = apiConProps.getProperty(PropertyNames.EndPoint) //datasource end point host
    val httpClient: CloseableHttpClient = HttpClients.custom().build() //building Http client for API connection
    val httpPostToken = new HttpPost(s"$identityEndpoint&client_id=$clientID&client_secret=$clientSecret")
    val securityToken = httpClient.execute(httpPostToken, new BasicResponseHandler()) //gets access token with certain expiration time

    val jValue = parse(securityToken)

    marketoCreds += (jValue \ "access_token").extract[String]
    marketoCreds += (jValue \ "scope").extract[String]


    marketoCreds //gets access token and scope
  }

  //making API call and getting response from it
  def performGet(filterType: String, filterValues: String, accessToken: String, fields: String): String = {

    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val activityEndpoint = apiConProps.getProperty(PropertyNames.LeadEndpoint)
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    println(s"$activityEndpoint?access_token=$accessToken&filterType=$filterType&filterValues=$filterValues&fields=$fields")
    val httpGetToken = new HttpGet(s"$activityEndpoint?access_token=$accessToken&filterType=$filterType&filterValues=$filterValues&fields=$fields")
    val response = httpClient.execute(httpGetToken, new BasicResponseHandler())

    response

  }


  def updatePostStatus(dataFrame: DataFrame, properties: Properties): Unit = {

    var dbCon: Connection = null
    var stmt: Statement = null


    val dfUpd = dataFrame.select("email", "id").toDF()
    DataUtilities.runPreparedStatement(
      properties,
      dfUpd,
      "UPDATE MAP_MKTO.MCT_MKTO_EMAIL_ACTIVITY " +
        "SET EMAIL = ?, " +
        "UPDATE_TS = CURRENT_TIMESTAMP " +
        "WHERE ACTIVITY_TYPE_ID = 9 and PROCESSED_FLAG = 'N' and EMAIL is NULL and  LEAD_ID = ?",
      dfUpd.columns, Array(0, 1), null, "UPDATE")

  }


  def updateStatusForNoEmail(dataFrame: DataFrame, properties: Properties): Unit = {
    var dbCon: Connection = null
    var stmt: Statement = null

    val dfUpd = dataFrame.select("ID").toDF()
    DataUtilities.runPreparedStatement(
      properties,
      dfUpd,
      "UPDATE MAP_MKTO.MCT_MKTO_EMAIL_ACTIVITY " +
        "set PROCESSED_FLAG = 'E',UPDATE_TS = CURRENT_TIMESTAMP " +
        "WHERE ACTIVITY_TYPE_ID = 9 and PROCESSED_FLAG = 'N'  and EMAIL is NULL and LEAD_ID = ?",
      dfUpd.columns,
      Array(0), null, "UPDATE")
  }

}
