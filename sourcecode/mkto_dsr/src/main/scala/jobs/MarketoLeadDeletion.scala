package jobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods.parse

import java.net.SocketTimeoutException
import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties


object MarketoLeadDeletion extends ETLFrameWork {

  //Initialization of variables
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var tgtTableName: String = null
  var logTableName: String = null
  var apiSource: String = null
  var deleteApiSource: String = null
  var dbSource: String = null
  var bException: Boolean = false
  var action: String = null
  var revertDF: DataFrame = null

  override def getPropertiesFromJson(json: String): Properties = super.getPropertiesFromJson(json)

  var lookupField: String = null
  var finalJoinedDFCount: Long = 0
  var count: Long = 0
  var count2: Long = 0
  var errorCounter: String = null
  var errorDesc: String = null
  var errorCode: String = null

  //Case class definition to extract from JSON response
  case class Result(id: Int, firstName: String, lastName: String, email: String, updatedAt: String, createdAt: String, IDM_ID: String)

  case class PostResponse(requestId: String, result: Seq[Result], success: Boolean)

  case class SyncResponse(requestId: String, success: Boolean, result: Seq[SyncResult], errors: Seq[Errors])

  case class SyncResult(status: String, reasons: Seq[Reasons])

  case class DeleteResponse(requestId: String, result: Seq[DeleteResult], success: Boolean)

  case class DeleteResult(id: Int, status: String, reasons: Option[Seq[Reasons]], errors: Seq[Errors])

  case class Reasons(code: String, message: String)

  case class Errors(code: String, message: String)


  // CALL IN MARKETO TOKEN - FIRST API
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

  // CALL IN LEADS BY IDM - SECOND API
  def getLeadsByIdm(token: String, idm_ids: String) = {
    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
    val leadEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    val httpGetRequest = new HttpGet(s"$leadEndpoint?access_token=$token&filterType=IDM_ID&filterValues=$idm_ids")
    httpGetRequest.addHeader("Content-type", "application/json;charset=UTF-8")
    httpGetRequest.addHeader("Accept", "application/json")
    val leads = httpClient.execute(httpGetRequest, new BasicResponseHandler())
    leads
  }

  //Code to build payload to POST to Marketo
  def buildPayload(action: String, lookUpField: String, input: String): String = {

    val payload =
      s"""{
         |  "action":"$action",
         |  "lookupField": "$lookUpField",
         |  "input": $input
         |}
         |""".stripMargin
    payload
  }

  // SYNC LEADS (CRM - false) - THIRD API
  def syncLeads(payload: String, token: String): String = {

    val apiConProps: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, apiSource)
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
        postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())
    }
    postResponse
  }

  // DELETE LEADS - FOURTH API
  def deleteLeads(token: String, leadIds: String): String = {
    val apiConProps: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, deleteApiSource)
    val leadDeleteEndpoint = apiConProps.getProperty(PropertyNames.ResourceSpecific_1)
    var postResponse: String = null
    val httpPostToken = null
    val httpClient: CloseableHttpClient = HttpClients.custom().build()

    //Exception handling for duplicates in Payload
    try {
      val httpPostToken = new HttpPost(s"$leadDeleteEndpoint?access_token=$token&id=$leadIds")
      httpPostToken.addHeader("Content-type", "application/json;charset=UTF-8")
      httpPostToken.addHeader("Accept", "application/json")
      postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())
    }
    catch {
      // Case statement
      case _: SocketTimeoutException =>
        postResponse = httpClient.execute(httpPostToken, new BasicResponseHandler())
    }
    postResponse
  }

  def getValue(x: Option[String]) = x match {
    case Some(s) => s
    case None => ""
  }

  def getRequestId(x: Option[Long]): Long = x match {
    case Some(s) => s
    case None => 0
  }

  def updateMarketoProcessedStatus(idmIds: String, statusFlag: String, idmLeads: Map[String, String]): Unit = {
    val dbConnectionInfo: String = AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint)
    var dbCon: Connection = null
    var stmt: Statement = null
    val idmIdList = idmIds.split(",")
    try {
      dbCon = DriverManager.getConnection(dbConnectionInfo, AppProperties.CommonDBConProperties)
      stmt = dbCon.createStatement
      for (idmId <- idmIdList) {
        val leads = getValue(idmLeads.get(idmId))
        var sql = ""
        if (leads.equals("")) {
          sql = s"UPDATE $tgtTableName SET MKTO_PROCESSED_FLAG = '$statusFlag', MKTO_PROCESSED_TS = CURRENT TIMESTAMP WHERE IDM_ID = $idmId"
        } else {
          sql = s"UPDATE $tgtTableName SET MKTO_PROCESSED_FLAG = '$statusFlag', MKTO_LEAD_IDS= $leads,  MKTO_PROCESSED_TS = CURRENT TIMESTAMP WHERE IDM_ID = $idmId"
        }
        stmt.executeUpdate(sql)
      }
      dbCon.commit()
    }
    finally {
      if (dbCon != null) dbCon.close()
    }
  }

  def getPayloadInput(id: Int): String = {
    val input =
      s"""{
         |      "SynctoMarketo__c":false,
         |      "id": $id
         |    }
         |    """.stripMargin
    input
  }

  def updateMarketoLeads(idmId: String, leads: String): Unit = {
    val dbConnectionInfo: String = AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint)
    var dbCon: Connection = null
    var stmt: Statement = null
    val sql = s"(UPDATE $tgtTableName SET MKTO_LEAD_IDS = $leads WHERE IDM_ID=$idmId ) as result"
    try {
      dbCon = DriverManager.getConnection(dbConnectionInfo, AppProperties.CommonDBConProperties)
      stmt = dbCon.createStatement
      stmt.executeUpdate(sql)
      dbCon.commit()
    }
    finally {
      if (dbCon != null) dbCon.close()
    }
  }

  def logDeleteResponse(postDeleteResponse: DeleteResponse, leadRequests: Map[String, Long]): Unit = {
    val dbConnectionInfo: String = AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint)
    var dbCon: Connection = null
    var stmt: Statement = null
    var sql = ""
    val results = postDeleteResponse.result
    try {
      dbCon = DriverManager.getConnection(dbConnectionInfo, AppProperties.CommonDBConProperties)
      stmt = dbCon.createStatement
      for (result <- results) {
        //get Result key based on lead id
        val requestKey = getRequestId(leadRequests.get(result.id.toString))
        if (result.status.equals("deleted")) {
          sql =
            s""" INSERT INTO $logTableName (REQUEST_ID, CONFIG_ID, SCHEMA_NAME, TABLE_NAME, KEY_NAME, KEY_VALUE, DELETED_ROWS)
               |VALUES ($requestKey,0,'Marketo','DELETE_API','leadId',${result.id},1)""".stripMargin
        } else {
          sql =
            s""" INSERT INTO $logTableName (REQUEST_ID, CONFIG_ID, SCHEMA_NAME, TABLE_NAME, KEY_NAME, KEY_VALUE, DELETED_ROWS)
               |VALUES ($requestKey,0,'Marketo','DELETE_API','leadId',${result.id},0)""".stripMargin
        }
        stmt.executeUpdate(sql)
      }
      dbCon.commit()
    }
    finally {
      if (dbCon != null) dbCon.close()
    }
  }

  override def getDataSourceDetails(spark: SparkSession, dataSourceCode: String): Properties = super.getDataSourceDetails(spark, dataSourceCode)

  def main(args: Array[String]): Unit = {
    //Args to the job
    tgtTableName = args(args.indexOf("--tgtTable") + 1)
    logTableName = args(args.indexOf("--logTable") + 1)
    apiSource = args(args.indexOf("--apiSource") + 1)
    deleteApiSource = args(args.indexOf("--deleteApiSource") + 1)
    dbSource = args(args.indexOf("--dbSource") + 1)
    action = args(args.indexOf("--action") + 1)
    lookupField = args(args.indexOf("--lookupField") + 1)

    log.info("Initialization started")
    this.initializeFramework(args)
    log.info("Initialization completed.")
    log.info(s"Starting ETL Job => $jobClassName....")
    log.info(s"apiSource => $apiSource")
    log.info(s"deleteApiSource => $deleteApiSource")

    val spark: SparkSession = AppProperties.SparkSession
    log.info("ETL logic goes here...")
    try {
      DataUtilities.recordJobHistory(spark, jobClassName, Constants.JobStarted)
      val srcCon = DataUtilities.getDataSourceDetails(spark, dbSource)
      val sql = s"(SELECT IDM_ID, ID FROM $tgtTableName WHERE DB_PROCESSED_FLAG = 'Y' and MKTO_PROCESSED_FLAG = 'N' and MKTO_PURGE_ENABLE='Y' ) as result"
      val dfTgtData = spark.read
        .jdbc(srcCon.getProperty(PropertyNames.EndPoint), sql, srcCon)
      dfTgtData.show()
      val idmIds = dfTgtData.collect.map(row => row.getLong(0)).mkString(",")
      val idmMap = dfTgtData.collect().map(row => row.getAs[Long](0) -> row.getAs[Long](1)).toMap
      var flowComplete = true
      var leadIds = ""
      // get marketo api token
      log.info("Get Marketo Token API call")
      var token = getMarketoToken
      log.info(s"API token =>  $token")

      //get lead ids by passing DSR IDM ids
      log.info("Get Leads API call")
      var leadResponse = getLeadsByIdm(token, idmIds)

      if (leadResponse.contains("Access token expired") || leadResponse.contains("Access token invalid")) {
        token = getMarketoToken
        leadResponse = getLeadsByIdm(token, idmIds)
      }
      log.info("Parsing the response from Marketo Get Leads")
      val jValue = parse(leadResponse)
      // create a postResponse object from the response
      val postResponse = jValue.extract[PostResponse]
      if (postResponse.success & postResponse.result.length > 0) {
        val leads: Seq[Result] = postResponse.result
        var payloadInput = "["
        var leadIds = ""
        var idmLeads: Map[String, String] = Map[String, String]()
        var leadRequests: Map[String, Long] = Map[String, Long]()
        for (lead <- leads) {
          if (idmLeads.contains(lead.IDM_ID)) {
            idmLeads += (lead.IDM_ID -> idmLeads(lead.IDM_ID).concat(lead.id.toString))
          } else {
            idmLeads += (lead.IDM_ID -> lead.id.toString)
          }

          if (!leadRequests.contains(lead.id.toString)) {
            //get request key based on idm id
            val requestId = getRequestId(idmMap.get(lead.IDM_ID.toLong))
            leadRequests += (lead.id.toString -> requestId)
          }

          if (payloadInput.equals("[")) {
            leadIds = leadIds + lead.id
            payloadInput = payloadInput + " " + getPayloadInput(lead.id)
          } else {
            leadIds = leadIds + "," + lead.id
            payloadInput = payloadInput + "," + getPayloadInput(lead.id)
          }
        }
        payloadInput = payloadInput + " " + "]"
        //sync lead
        val payload = buildPayload(action, lookupField, payloadInput)
        log.info("Sync Leads API call")
        var syncResponse = syncLeads(payload, token)
        if (syncResponse.contains("Access token expired") || syncResponse.contains("Access token invalid")) {
          token = getMarketoToken
          syncResponse = syncLeads(payload, token)
        }
        log.info("Parsing the response from Sync API")
        val jSyncValue = parse(syncResponse)
        val postSyncResponse = jSyncValue.extract[SyncResponse]
        log.info(s"syncResponse => $syncResponse")
        if (postSyncResponse.success) {
          //delete lead
          log.info("Delete Leads API call")
          val deleteResponse = deleteLeads(token, leadIds)
          log.info(s"deleteResponse => $deleteResponse")
          val deletedResponseJson = parse(deleteResponse)
          // create a postResponse object from the response
          val postDeleteResponse = deletedResponseJson.extract[DeleteResponse]
          logDeleteResponse(postDeleteResponse, leadRequests)
          if (postDeleteResponse.success) {
            flowComplete = false
            updateMarketoProcessedStatus(idmIds, "Y", idmLeads)
          } else {
            updateMarketoProcessedStatus(idmIds, "N", idmLeads)
          }
        }
      }
    }

    catch {
      case e: Throwable =>
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
    }
    finally {
      if (bException) { // Job failed
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobFailed, s"e.getMessage - e.getCause", null, null)
      } else {
        DataUtilities.recordJobHistory(AppProperties.SparkSession, jobClassName, 0, Constants.JobSucceeded, "Job completed without any error", null, null)
      }
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")
    }
  }

}