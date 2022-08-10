package jobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.spark.sql.DataFrame
import org.json.{JSONArray, JSONException, JSONObject}
import java.io.{IOException, _}
import java.net.URL
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util._

import javax.net.ssl.HttpsURLConnection


object CDStoMIP extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var isJobFailed = false


  // Static values for creating api call
  private var cdsEndPoint: String = ""
  private var apiKey: String = ""
  private val page: String = "1"
  private var size: String = ""
  private var extractedPagesCount: Int = 0
  var countSoFar = 0
  var retryCount = 0

  //argument variables
  private var defaultMinTimestamp = "" // Timestamp of last full database refresh, set at getArgs()
  private var lastRunTimestamp: String = null
  private var MINIMUM_TIMESTAMP_OFFSET = 0
  private var MAXIMUM_TIMESTAMP_OFFSET = 0
  private var MINIMUM_TIMESTAMP_OFFSET_MINUTES = 0
  private var mergeSql = ""


  // Will hold all the Json to create the array in the end
  private var cdsJSONFull: JSONArray = new JSONArray()


  // Database Endpoint variables
  private var MIP_ENDPOINT = ""
  private var CORE_SCHEMA = ""
  private var CDS_ASSET_TABLE = ""

  /*
  Steps for Job:
  1) Pull data from CDS API endpoint ( callCDSAPI() )
  2) Create dataframe from parsed json from endpoint ( parseJson() + jsonToDataframe() )
  3) Run prepared statement to update/insert CDS data in MIP Database ( rows in MAP_CORE.MCT_CDS_ASSET )
   */
  @throws(classOf[Exception])
  def runJobSequence_CDStoMIP(): Unit = { // NOSONAR
    log.info("runJobSequence started")
    // ETL Logic goes here
    /* Note: You have access to the following instances:
        - CommonDBConProperties (connection properties to connect to App Config DB)
        - commonJobSeqCode (job sequence code)
        - sparkSession (spark session instance)

        At this point, you should have access to Common functions
        e.g: this.getDataSourceDetails(sparkSession, dataSourceCode = "ISAP")

        If you need to extract and save/write data, then use the Object: DataUtilities
        This object is a facade to all functions available.
        e.g: DataUtilities.readDataWithColumnPartitioning(...)
    */


    // For tracking runtime of job
    val startTime = System.currentTimeMillis()
    val startDate = new Date(startTime * 1000L)
    log.info("start time: " + startDate)

    // Needed to avoid database lockouts from multiple partitions trying to create connections at the same time.
    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 2000

    log.info("Getting MIP data properties and setting sslConnection to true.")
    val MIPdbProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession,MIP_ENDPOINT)
    MIPdbProperties.setProperty("sslConnection", "true")

    log.info("Getting Last Successful Run Time")
    val successfulTimestamp = DataUtilities.getLastSuccessfulRunTime(AppProperties.SparkSession, "CDStoMIP")
    if (!successfulTimestamp.isEmpty) {
      val char = successfulTimestamp("JOB_START_TIME").lastIndexOf("-")
      lastRunTimestamp = successfulTimestamp("JOB_START_TIME")
        .replaceAll("[.]", ":")
      lastRunTimestamp = lastRunTimestamp.substring(0,char) + " " + lastRunTimestamp.substring(char+1)
    }
    log.info(s"Last Successful Timestamp: $lastRunTimestamp")

    log.info("Looking backwards " + MINIMUM_TIMESTAMP_OFFSET + " days.")
    log.info("Looking backwards " + MINIMUM_TIMESTAMP_OFFSET_MINUTES + " minutes.")
    log.info("Looking forwards " + MAXIMUM_TIMESTAMP_OFFSET + " days.")

    val minTimeStamp = getFormattedTimeStamp("min")
    val maxTimeStamp = getFormattedTimeStamp("max")

    log.info("Minimum timestamp formatted: " + minTimeStamp)
    log.info("Maximum timestamp formatted: " + maxTimeStamp)

    // Built api call
    var cdsURL = cdsEndPoint + "?apiKey=" + apiKey + "&page=" + page + "&size=" + size + "&minUpdateTs=" + minTimeStamp + "&topUpdateTs=" + maxTimeStamp

    println(cdsURL)

    // Needs to be written this way to avoid StackOverflow (instead of a recursive function due to potentially large amount of calls)
    while ( {
      cdsURL != ""
    }) {
      cdsURL = callCDSAPI(cdsURL);
    }
    log.info("Creating CDS dataframe...")
    val cdsDF = jsonToDataFrame(cdsJSONFull.toString)
    cdsJSONFull = new JSONArray()
    cdsDF.cache()


    //log.info("Perform dataframe transformations for formatting...")

    if (!cdsDF.isEmpty) {
      log.info("Dropping null UUC_ID's...")
      // Drop any columns with null UUC_ID values.
      val df = cdsDF.na.drop(Seq("UUC_ID"))
      df.cache()
      df.show()


      log.info("Performing Update/Insert of CDS data....")


      DataUtilities.runPreparedStatement(
        MIPdbProperties,
        df,
        mergeSql,
        df.columns,
        null,
        null,
        null)

    }

    val totalTime: Long = (System.currentTimeMillis()-startTime)/(1000*60)

    log.info("Total time: "  + totalTime + "  minutes.")

    log.info("runJobSequence ended.")
  }

  /*
   Calls the CDS API using the api built string that is passed in.
   Gets a JSON of the CDS values that is then filtered and parsed into another JSON that is then passed into a Dataframe
   in a later call.
   */
  private def callCDSAPI(urlString: String): String = {
    log.info("Starting callCDSAPI")
    var cdsJSON: JSONObject = null

    try {
      log.info("Getting JSON")
      val urlConnection = new URL(urlString).openConnection.asInstanceOf[HttpsURLConnection]
      urlConnection.setRequestMethod("GET")
      urlConnection.setRequestProperty("accept", "application/json")

      val responseCode = urlConnection.getResponseCode
      println(responseCode)
      if (responseCode == 404) {
        log.info("No data withing provided date range")
        return ""
      }
      if (responseCode != 200) {
        log.info("Unexpected network error occurred...retrying")
        //returns the same urlString back to try again
        retryCount += 1
        println("Retry count: " + retryCount)
        if (retryCount > 10) {
          log.info("Retry count exceeding 10 tries, throwing exception")
          throw new IOException("doGet Error - HTTP Status: " + responseCode)
        }
        return urlString
      }
      if (responseCode == 200) {
        retryCount = 0
        log.info("Response code 200, parsing data")
        cdsJSON = getJSONOBJECT(urlConnection)
        return parseData(cdsJSON, extractedPagesCount)
      } else  {
        throw new IOException("doGet Error - HTTP Status: " + responseCode)
      }
    } catch {
      case e: Exception =>
        log.error("[REST][GET] Error when executing GET method.", e)
        throw new Exception("[REST][GET] Error when executing GET method.", e)

    }
  }

  private def getJSONOBJECT(urlConnection: HttpsURLConnection): JSONObject = {
    val inputStream: InputStream = urlConnection.getInputStream
    val cdsJSON = new JSONObject(convertStreamToString(inputStream))
    cdsJSON
  }

  private def convertStreamToString(stream: InputStream): String ={
    try {
      return new Scanner(stream).useDelimiter("\\A").next()
    } catch {
      case e: NoSuchElementException =>
        return ""
    }
  }


  private def parseData(jsonObject: JSONObject, count: Int): String = {
    var nextPageLink = ""
    var pageCount = count
    try {
      val jsonDataArray: JSONArray = jsonObject.getJSONArray("data")
      extractedPagesCount = Integer.parseInt(jsonObject.get("totalCount").toString)
      pageCount = pageCount + jsonDataArray.length
      countSoFar = countSoFar + jsonDataArray.length
      var i = 0
      log.info("Page count: " + pageCount)
      while ( {
        i < jsonDataArray.length
      }) {
        var ovCode = ""
        val relationshipDetailsArray = jsonDataArray.getJSONObject(i).getJSONArray("relationshipDetails")
        var j = 0
        while ( {
          j < relationshipDetailsArray.length
        }) {
          val relationshipDetailsData = relationshipDetailsArray.getJSONObject(j)
          if (relationshipDetailsData.get("contentTypeId").equals("OV")) {
            ovCode = relationshipDetailsData.get("contentId").toString
          }
          j += 1
        }
        val ovCodeCoreAttributes = jsonDataArray.getJSONObject(i).get("coreAttributes").asInstanceOf[JSONObject]

        val universalContentId = ovCodeCoreAttributes.get("universalContentId")
        val contentTitle = ovCodeCoreAttributes.get("contentTitle")
        val contentUrl = ovCodeCoreAttributes.get("contentUrl")
        val dlvryUrl = ovCodeCoreAttributes.get("dlvryUrl")
        val dlvryUrlId = ovCodeCoreAttributes.get("dlvryUrlId")
        val langCd = ovCodeCoreAttributes.get("langCd");
        var ctryCd = ovCodeCoreAttributes.get("ctryCd")
        if (ctryCd.toString != "null") {
          ctryCd = ctryCd.toString.substring(0,2); // Have to cut country code down as sometimes you get length 3
        }
        val ut10Cd = ovCodeCoreAttributes.get("ut10Cd");
        val ut15Cd = ovCodeCoreAttributes.get("ut15Cd");
        val ut17Cd = ovCodeCoreAttributes.get("ut17Cd");
        val ut20Cd = ovCodeCoreAttributes.get("ut20Cd");
        val ut30Cd = ovCodeCoreAttributes.get("ut30Cd");
        val contentType = ovCodeCoreAttributes.get("contentType");
        val contentFormat = ovCodeCoreAttributes.get("contentFormat");


        val JsonValues = new JSONObject()
          .put("UUC_ID", universalContentId)
          .put("ASSET_DEFAULT_TITLE", contentTitle)
          .put("CONTENT_URL", contentUrl)
          .put("DLVRY_URL", dlvryUrl)
          .put("DLVRY_URL_ID", dlvryUrlId)
          .put("UT10_CODE", ut10Cd)
          .put("UT15_CODE", ut15Cd)
          .put("UT17_CODE", ut17Cd)
          .put("UT20_CODE", ut20Cd)
          .put("UT30_CODE", ut30Cd)
          .put("COUNTRY_CODE", ctryCd)
          .put("LANG_CODE", langCd)
          .put("CONTENT_TYPE_ID", contentType)
          .put("OV_CODE", ovCode)
          .put("CONTENT_FORMAT_CD", contentFormat)


        // TODO need entity.name (from title)

        cdsJSONFull.put(JsonValues)

        i += 1
      }
      if (jsonObject.has("nextPageLink")) {
        nextPageLink = jsonObject.getString("nextPageLink")
        val subPageLink = nextPageLink.substring(8, nextPageLink.length).replaceAll(" ", "%20").replaceAll(":", "%3A")
        nextPageLink = nextPageLink.substring(0, 8) + subPageLink
        log.info("count so far : " + countSoFar + "+++++ nextPageLink: " + nextPageLink)
        val remaining = extractedPagesCount - countSoFar
        log.info("count remaining : " + remaining)
        return nextPageLink
      }
      else {
        log.info("count so far : " + countSoFar + "+++++ lastPage")
        log.info("Total count : " + extractedPagesCount)
        return ""
      }
    } catch {
      case e: JSONException =>
        e.printStackTrace()
        throw new Exception(e)
    }
  }

  def jsonToDataFrame(jsonString: String):DataFrame ={
    val spark = AppProperties.SparkSession
    import spark.implicits._
    val df = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(jsonString)).toDS())
    df
  }

  /*
     * Formats the timestamp needed for the delta API call, mode can be min or max
     * for min timestamp or max timestamp.
     */
  def getFormattedTimeStamp(mode: String): String = {
    var timestamp: Timestamp = null
    if (mode == "min") {
      // Uses defaultMinTimestmap or timestamp of job last ran
      if (lastRunTimestamp == null) {
        timestamp = Timestamp.valueOf(defaultMinTimestamp)
      } else {
        timestamp = Timestamp.valueOf(lastRunTimestamp)
      }
      val instant = timestamp.toInstant
      timestamp = Timestamp.from(instant.minus(MINIMUM_TIMESTAMP_OFFSET, ChronoUnit.DAYS).minus(MINIMUM_TIMESTAMP_OFFSET_MINUTES, ChronoUnit.MINUTES))
      // Set timestamp to a 1 day offset in the past to accommadate CMDP difference if using full refresh.
    } else if (mode == "max") {
      // 1 day future in order to make sure time range goes all the way to current as leeway
      timestamp = Timestamp.from(Instant.now().plus(1, ChronoUnit.DAYS).minus(MAXIMUM_TIMESTAMP_OFFSET, ChronoUnit.DAYS))
    }
    log.info(mode + " timestamp is: " + timestamp)
    val pattern = """[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}""".r
    val find = pattern.findFirstIn(timestamp.toString)
    var timestampString = find.toString.replaceAll(" ", "%20").replaceAll(":", "%3A")
    timestampString = timestampString.substring(5, timestampString.length - 1)
    timestampString
  }

  /*
   Searches the command line arguments for the baseDB argument and defaultruntimestamp.
   Looks for "--baseDB" and then assumes the MIP database argument will be the index after.
   Needed for knowing which MIP database to put in for the DataUtilities connection details.
   Similarly looks for index for default runtimestmap "--defaultRunTime".
   */
  private def getArgs(args: Array[String]): Unit = {

    if (args.length % 2 == 0) {
      MIP_ENDPOINT = args(args.indexOf("--baseDB") + 1)
      CORE_SCHEMA = args(args.indexOf("--trgtSchema") + 1)
      CDS_ASSET_TABLE = args(args.indexOf("--trgtTable") + 1)
      defaultMinTimestamp = args(args.indexOf("--defaultRunTime") + 1)
      size = args(args.indexOf("--pageSize") + 1)
      MINIMUM_TIMESTAMP_OFFSET = args(args.indexOf("--minOffset") + 1).toInt
      MAXIMUM_TIMESTAMP_OFFSET = args(args.indexOf("--maxOffset") + 1).toInt
      MINIMUM_TIMESTAMP_OFFSET_MINUTES = args(args.indexOf("--minMins") + 1).toInt
      mergeSql = args(args.indexOf("--mergeSql") +1)
      val cds_details = args(args.indexOf("--cdsDetails") +1)
      // get information for CDS Endpoint
      val cds_conn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, cds_details)
      cdsEndPoint = cds_conn.getProperty(PropertyNames.EndPoint)
      apiKey = cds_conn.getProperty(PropertyNames.ClientSecret)

    } else {
      throw new IllegalArgumentException(
        "Uneven amount of argument key-value pairs provided."
      )
    }

  }

  def main(args: Array[String]): Unit = {
    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      // Gets the command line argument for the BaseMIP database (ex: MIP_DEV for dev database) and defaultMinTimestamp
      getArgs(args)

      // Log job status START - DB
      // log.info(s"CommonDBConnProperties => ${this.CommonDBConProperties}")
      // log.info(s"Log to JobHistoryLogTable => ${AppProperties.JobHistoryLogTable}")
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobStarted,
        s"CDStoMIP Started with minOffset: $MINIMUM_TIMESTAMP_OFFSET days and maxOffset: $MAXIMUM_TIMESTAMP_OFFSET days",null,null)


      log.info("Starting ETL...")

      runJobSequence_CDStoMIP()
      /* Note: You have access to the following instances:
        - CommonDBConProperties (connection properties to connect to App Config DB)
        - commonJobSeqCode (job sequence code)
        - sparkSession (spark session instance)

        At this point, you should have access to Common functions
        e.g: this.getDataSourceDetails(sparkSession, dataSourceCode = "ISAP")

        If you need to extract and save/write data, then use the Object: DataUtilities
        This object is a facade to all functions available.
        e.g: DataUtilities.readDataWithColumnPartitioning(...)
      */

      // Log job status POST - DB
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobSucceeded,
        s"CDStoMIP Interface completed - $countSoFar rows extracted",null,null)

      log.info(s"Completed Job => $jobClassName.")

    } catch {
      case e:Throwable=>
        isJobFailed = true
        e.printStackTrace()
        log.error(e.getMessage +" - "+e.getCause)
        DataUtilities.recordJobHistory(AppProperties.SparkSession,
          AppProperties.CommonJobSeqCode,
          0,
          Constants.JobFailed,
          e.getMessage + " - " + e.getCause,null,null)
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")

      if (isJobFailed) {
        System.exit(1)
      }
    }
  }

}

