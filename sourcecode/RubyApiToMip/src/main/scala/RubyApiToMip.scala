import com.ibm.mkt.etlframework.audit.JobRunArgs
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.http.client.HttpResponseException
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.apache.spark.sql.functions.{desc, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager}
import scala.collection.mutable.Map

object RubyApiToMip extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var bException: Boolean = false
  var CmpnSource: String = null
  var CmpnMetaSource: String = null
  var lastRun = ""
  var dbTgtSource: String = null
  var fetchedTS: String = null

  def main(args: Array[String]): Unit = {
    try {
      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobStarted)

      dbTgtSource = args(args.indexOf("--dbTgtSource") + 1)
      CmpnSource = args(args.indexOf("--CmpnSource") + 1)
      CmpnMetaSource = args(args.indexOf("--CmpnMetaSource") + 1)

      val targetDB = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, dbTgtSource)
      val dbConnectionInfo = targetDB.getProperty(PropertyNames.EndPoint)
      val dbCon = DriverManager.getConnection(dbConnectionInfo, targetDB)

      var runDate: String = null
      val runDateIndex = args.indexOf("--runDate")

      if (runDateIndex >= 0) {
        log.info(s"Arg 'runDate' found at index: $runDateIndex")
        if (args.length > (runDateIndex + 1)) {
          runDate = args(runDateIndex + 1)
          log.info(s"Value for arg 'runDateIndex' value: $runDate")
        } else {
          throw new IllegalArgumentException(
            "Value for arg 'runDate' could not be found. Please pass the value."
          )
        }
      }

      if (runDate == null || runDate == "" || runDate == "null") {
        lastRun = getLastRecordTimeStamp(jobClassName).toString
        if (lastRun == null || lastRun == "" || lastRun == "null") {
          log.info("Both lastRun and runDate are empty. Please provide runDate. {}", lastRun)
        } else {
          runDate = lastRun
          log.info("Running using lastRun {}", lastRun)
        }
      } else {
        log.info("Running using  runDate {}", runDate)
      }

      val tgtsqlStr = """MERGE INTO MAP_CORE.MCT_RUBY_CAMPAIGN t USING TABLE ( VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ) as v (CMPN_CD,CMPN_UT_10,CMPN_UT_15,CMPN_UT_17,CMPN_UT_20,CMPN_UT_30,PLAN_NAME,PLAN_CD,PLAN_TYPE_CD,SUB_PLAN_NAME,SUB_PLAN_CD,SUB_PLAN_UT_15,GLOBAL_CMPN_NAME,GLOBAL_CMPN_CD,GLOBAL_CMPN_TYPE_CD,ACTV_FLG) ON t.CMPN_CD = v.CMPN_CD WHEN MATCHED THEN UPDATE SET t.CMPN_UT_10 = v.CMPN_UT_10,t.CMPN_UT_15 = v.CMPN_UT_15,t.CMPN_UT_17 = v.CMPN_UT_17,t.CMPN_UT_20 = v.CMPN_UT_20,t.CMPN_UT_30 = v.CMPN_UT_30,t.PLAN_NAME = v.PLAN_NAME,t.PLAN_CD = v.PLAN_CD,t.PLAN_TYPE_CD = v.PLAN_TYPE_CD,t.SUB_PLAN_NAME = v.SUB_PLAN_NAME,t.SUB_PLAN_CD = v.SUB_PLAN_CD,t.SUB_PLAN_UT_15 = v.SUB_PLAN_UT_15,t.GLOBAL_CMPN_NAME = v.GLOBAL_CMPN_NAME,t.GLOBAL_CMPN_CD = v.GLOBAL_CMPN_CD,t.GLOBAL_CMPN_TYPE_CD = v.GLOBAL_CMPN_TYPE_CD,t.ACTV_FLG = v.ACTV_FLG WHEN NOT MATCHED THEN INSERT (CMPN_CD,CMPN_UT_10,CMPN_UT_15,CMPN_UT_17,CMPN_UT_20,CMPN_UT_30,PLAN_NAME,PLAN_CD,PLAN_TYPE_CD,SUB_PLAN_NAME,SUB_PLAN_CD,SUB_PLAN_UT_15,GLOBAL_CMPN_NAME,GLOBAL_CMPN_CD,GLOBAL_CMPN_TYPE_CD,ACTV_FLG) VALUES (v.CMPN_CD,v.CMPN_UT_10,v.CMPN_UT_15,v.CMPN_UT_17,v.CMPN_UT_20,v.CMPN_UT_30,v.PLAN_NAME,v.PLAN_CD,v.PLAN_TYPE_CD,v.SUB_PLAN_NAME,v.SUB_PLAN_CD,v.SUB_PLAN_UT_15,v.GLOBAL_CMPN_NAME,v.GLOBAL_CMPN_CD,v.GLOBAL_CMPN_TYPE_CD,v.ACTV_FLG)""".stripMargin
      runJobSequence(tgtsqlStr, dbCon, runDate)
      //DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobSucceeded)
      val jobrun = new JobRunArgs
      //val jobrun:JobRunArgs = new JobRunArgs()
      jobrun.jobSpecific1 = fetchedTS
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobSucceeded, jobrun)
      log.info(s"Completed Job => $jobClassName.")
      println("fetchedTS===> " + fetchedTS)
    } catch {
      case e: Throwable => {
        bException = true
        e.printStackTrace
        log.error(e.getMessage + " - " + e.getCause)
        DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "", Constants.JobFailed)
      }
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")

      if (bException) {
        System.exit(1)
      }
    }
  }

  def runJobSequence(tgtsqlStr: String, targetDB: Connection, runDate: String): Unit = {
    log.info("runJobSequence started.")
    try {
      val targetDF = extractor(AppProperties.SparkSession, runDate)
      targetDF.show(false)
      if (targetDF.isEmpty) {
        log.info(s"Target Data set is empty...")
      } else {
        loadDataToDB(AppProperties.SparkSession, targetDB, tgtsqlStr, targetDF)
      }
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        bException = true
      }

        log.info("runJobSequence ended.")
    }
  }

  def extractor(spark: SparkSession, runDate: String): DataFrame = {
    log.info("extractor has triggered.")
    val codes = getCampaignCodes(runDate) //"2021-06-01"
    val campaignDF = parseCampaignData(spark, codes)
    campaignDF.show(false)
    fetchedTS = campaignDF.orderBy(desc("updated_date")).first().getString(3).split("\\.")(0)

    import spark.implicits._
    val metaDF = if (!campaignDF.isEmpty) {
      val activeFlagYCodes = campaignDF
        //.filter(col("active_flag") === "Y")
        .select("code").as[String].collect()
      //val codesArr = Array("AMMJN", "000039TA")

      val startTime = System.currentTimeMillis()
      val camMeta = getCampaignMetaData(activeFlagYCodes)
      val endTime = System.currentTimeMillis()
      val processTime = (endTime - startTime) / 1000

      log.info("Time taken to proess all Campaing Code = {}", processTime)
      val meta = if (camMeta.size != 0) {
        parseCampaignMetaData(spark, camMeta)
      } else {
        spark.emptyDataFrame
      }
      meta
    } else {
      spark.emptyDataFrame
    }
    metaDF.printSchema()
    metaDF
  }

  def loadDataToDB(spark: SparkSession, targetDB: Connection, tgtsqlStr: String, tgtDF: DataFrame): Unit = {
    DataUtilities.runPreparedStatementUsingConnection(targetDB, tgtDF, tgtsqlStr, tgtDF.columns, null, null, true, null, "merge")
  }


  def getCampaignCodes(lastRunTimestamp: String): String = {
    log.info("Get Campaign Code logic started.")
    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, CmpnSource)
    val activityEndpoint = apiConProps.getProperty(PropertyNames.EndPoint) // This will provide the Api url
    val activityId = apiConProps.getProperty(PropertyNames.ClientSecret)
    val leadEndpoint = apiConProps.getProperty(PropertyNames.LeadEndpoint) //This will provide rest of the path

    //println(s"RUBY_API_CMPN_URL=$activityEndpoint$leadEndpoint?apikey=$activityId&timestamp=$lastRunTimestamp")
    var requiredDateTime = lastRunTimestamp.replace(":", "%3A").replace(" ", "%20")
    log.info("Campaign Url : {}", s"$activityEndpoint$leadEndpoint?apikey=$activityId&timestamp=$requiredDateTime")
    val httpClient: CloseableHttpClient = HttpClients.custom().build()
    val httpGetToken = new HttpGet(
      s"$activityEndpoint$leadEndpoint?apikey=$activityId&timestamp=$requiredDateTime")
    var response = ""
    try {
      response = httpClient.execute(httpGetToken, new BasicResponseHandler())
    } catch {
      case ex: HttpResponseException =>
        println("No Codes found!.......\n" + ex.printStackTrace())
        log.info("No Codes Found! \n}", ex.printStackTrace())
        response = null
    }
    response
  }

  def getCampaignMetaData(campaign_codes: Array[String]): Map[String, String] = {
    log.info("Meta data api call started")
    val apiConProps = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, CmpnMetaSource)
    val activityEndpoint = apiConProps.getProperty(PropertyNames.EndPoint)
    val activityId = apiConProps.getProperty(PropertyNames.ClientSecret)
    val leadEndpoint = apiConProps.getProperty(PropertyNames.LeadEndpoint)
    val campaignsMap = Map[String, String]()

    for (campaign_code <- campaign_codes) {
      val startTimeMillis = System.currentTimeMillis()
      val url = s"$activityEndpoint$leadEndpoint?apikey=$activityId&campaign_code=$campaign_code"
      log.info("Meta data Url : {}", url)


      val httpClient: CloseableHttpClient = HttpClients.custom().build()
      val httpGetToken = new HttpGet(url)
      var response = ""
      try {
        response = httpClient.execute(httpGetToken, new BasicResponseHandler())
      } catch {
        case ex: HttpResponseException =>
          //println(s"No meta data found for $campaign_code.\n" + ex.printStackTrace())

          log.info(s"No meta data found for $campaign_code.\n {}", ex.printStackTrace())
      }

      val endTimeMillis = System.currentTimeMillis()
      val timeTakenSec = (endTimeMillis - startTimeMillis) / 1000
      // println(s"Time taken to process $campaign_code = $timeTakenSec")
      log.info(s"Time taken to process $campaign_code = {}", timeTakenSec)
      if (response != "") {
        campaignsMap += (campaign_code -> response)
      }
    }
    val dt = java.time.Instant.now()

    println("timestamp:" + dt)


    campaignsMap
  }

  def parseCampaignData(spark: SparkSession, response: String): DataFrame = {
    val parsedDF = if (response != null) {
      import spark.implicits._
      val df = spark.read
        .json(Seq(response).toDS())
      df.withColumn("data", explode($"data"))
        .select("data.campaign.code", "data.campaign.active_flag", "data.campaign.created_date", "data.campaign.updated_date")
    } else {
      spark.emptyDataFrame
    }
    parsedDF
  }

  def parseCampaignMetaData(spark: SparkSession, campaignsMap: Map[String, String]): DataFrame = {
    import spark.implicits._
    val dd = campaignsMap.values.toSeq
    val df = spark.read.json(dd.toDS())
    val parsedDF = df.withColumn("data", explode($"data"))
      .selectExpr("data.campaign_new.code as CMPN_CD", "data.campaign_new.name as GLOBAL_CMPN_NAME",
        "data.campaign_new.active_flag as ACTV_FLG", "data.campaign_old.code as old_campaign_code",
        "data.ut_codes.ut_10 as CMPN_UT_10", "data.ut_codes.ut_15 as CMPN_UT_15",
        "data.ut_codes.ut_17 as CMPN_UT_17", "data.ut_codes.ut_20 as CMPN_UT_20",
        "data.ut_codes.ut_30 as CMPN_UT_30", "data.global_campaign.code as GLOBAL_CMPN_CD",
        "data.global_campaign.type_code as GLOBAL_CMPN_TYPE_CD", "data.global_campaign.name as global_campaign_name",
        "data.sub_plan.code as SUB_PLAN_CD", "data.sub_plan.type_code as sub_plan_code_type",
        "data.sub_plan.name as SUB_PLAN_NAME", "data.sub_plan.ut_15_code as SUB_PLAN_UT_15",
        "data.plan.code as PLAN_CD", "data.plan.type_code as PLAN_TYPE_CD", "data.plan.name as PLAN_NAME")
      .select("CMPN_CD", "CMPN_UT_10", "CMPN_UT_15", "CMPN_UT_17", "CMPN_UT_20", "CMPN_UT_30", "PLAN_NAME", "PLAN_CD", "PLAN_TYPE_CD", "SUB_PLAN_NAME", "SUB_PLAN_CD", "SUB_PLAN_UT_15", "GLOBAL_CMPN_NAME", "GLOBAL_CMPN_CD", "GLOBAL_CMPN_TYPE_CD", "ACTV_FLG")
    parsedDF
  }

  @throws(classOf[Exception])
  def getLastRecordTimeStamp(jobSeqCode: String):String = {
    var dfResult: DataFrame = null
    var maxTs = ""
    try {
      dfResult = AppProperties.SparkSession.read
        .option("isolationLevel", Constants.DBIsolationUncommittedRead)
        .jdbc(AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint),
          s""" (SELECT
                       JOB_SK, JOB_SPECIFIC_1 AS MAX_TIMESTAMP
                    FROM MAP_ETL.ETL_JOB_HIST_V1
                    WHERE
                      SEQ_CODE = '$jobSeqCode'
                     AND JOB_STATUS not in ('Started','Failed')
                    ORDER BY JOB_SK DESC
                    FETCH FIRST ROW ONLY) AS RESULT_TABLE""",
          AppProperties.CommonDBConProperties)
      if (log.isDebugEnabled || log.isInfoEnabled())
        dfResult.show()
      if (dfResult.count() > 0) {
        val firstRow = dfResult.collect().head
        maxTs = firstRow.getString(1)
      }
     // else maxTs = null
    } finally {
      if (dfResult != null) dfResult.unpersist()
    }
    maxTs
  }


}
