package jobs

import com.google.common.base.Throwables
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.commons.net.ftp.{FTP, FTPReply, FTPSClient}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row}
import org.json.{JSONArray, JSONException, JSONObject}
import java.io.{BufferedReader, ByteArrayInputStream, IOException, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, Socket, URL}
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util._

import com.google.gson.JsonParser
import javax.net.ssl.{HttpsURLConnection, SSLSocket}

object CDSExtract extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var isJobFailed = false

  // Static values for pathing to FTP server
  private val FTP_HOST = "w3-transfer.boulder.ibm.com"
  private val FTP_BUCKET = "/www/prot/map_etl"
  private val FTP_ARCHIVE_BUCKET = "/www/prot/map_etl/archive"

  // Static values for pathing to COS Bucket
  private var ENV = ""
  private var COS_BUCKET = ""
  private val DEFAULT_IAM_ENDPOINT = "https://iam.cloud.ibm.com/identity/token?grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey="

  // Static values for creating api call
  private var cdsEndPoint: String = ""
  private var apiKey: String = ""
  private val page: String = "1"
  private val size: String = "200"
  private var pageCatCd: String = null
  private val ctryCd: String = "US"
  private var extractedPagesCount: Int = 0

  var countSoFar = 0
  var skipCounter = 0
  var retryCount = 0

  // Static values for pageCodes needed
  private val USProductCode: String = "PC030"
  private val USBlogCode: String = "PC090"



  // Will hold all the Json to create the array in the end
  private var cdsJSONFull: JSONArray = new JSONArray()

  // Used for building the csv file string
  private var CDS_COS_String = new StringBuilder()

  // Timestamps for dealing with detla call
  private var defaultMinTimestamp = "" // Timestamp of last full database refresh, set at getArgs()
  private var lastRunTimestamp: String = null

  // Database Endpoint variables
  private var MIP_ENDPOINT = ""
  private val ETL_SCHEMA = "MAP_ETL"

  // Timestamp offsets
  private var MINIMUM_TIMESTAMP_OFFSET = 0
  private var MAXIMUM_TIMESTAMP_OFFSET = 0

  /*
      Steps for Job:
      1) Pull data from CDS API endpoint ( callCDSAPI() )
      2) Create dataframe from parsed json from endpoint ( parseJson() + jsonToDataframe() )
      3) Truncate and load CDS data to MIP Database
      4) Create second dataframe that will be used to create csv ( createFinalDF() )
      5) Create csv (header + rows)
      6) Write csv to FTP server ( FTPUpload )
       */
  @throws(classOf[Exception])
  def runJobSequence_getCDSAttributes(pageCategoryCode: String, MIPdbProperties: Properties): Unit = { // NOSONAR
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

    pageCatCd = pageCategoryCode

    val minTimeStamp = getFormattedTimeStamp("min")
    val maxTimeStamp = getFormattedTimeStamp("max")

    println(minTimeStamp)
    println(maxTimeStamp)

    // Built api call
    var cdsURL = cdsEndPoint + "?apiKey=" + apiKey + "&page=" + page + "&size=" + size + "&minContentUpdtTs=" + minTimeStamp + "&topUpdateTs=" + maxTimeStamp + "&pageCatCd=" + pageCatCd + "&ctryCd=" + ctryCd
    println(cdsURL)
    // For tracking runtime of job
    val startTime = System.currentTimeMillis()
    val startDate = new Date(startTime * 1000L)
    log.info("start time: " + startDate)

    // Needed to avoid database lockouts from multiple partitions trying to create connections at the same time.
    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 2000




    while ( {
      cdsURL != ""
    }) {
      cdsURL = callCDSAPI(cdsURL);
    }

    log.info("Creating CDS dataframe...")
    val cdsDF = jsonToDataFrame(cdsJSONFull.toString)
    cdsJSONFull = new JSONArray()
    cdsDF.cache()
    cdsDF.show()


    if (cdsDF.isEmpty) {
      log.info("No records pulled, returning from job sequence.")
      return
    }

    var filtered_DF: DataFrame = null // NOSONAR

    if (pageCategoryCode == USProductCode) {
      log.info("Filtering urls for only products or us-en at www.ibm.com.")
      filtered_DF = cdsDF.filter("PAGE_URL like '%www.ibm.com%' AND (PAGE_URL like '%com/products%' or PAGE_URL like '%com/us-en%')")
      filtered_DF.cache()
    } else if (pageCategoryCode == USBlogCode) {
      log.info("Filtering urls for only www.ibm.com and removing download pages for blogs.")
      filtered_DF = cdsDF.filter("PAGE_URL LIKE '%www.ibm.com%' AND PAGE_URL NOT LIKE '%download%'")
      filtered_DF.cache()
    }


    if (filtered_DF.isEmpty || filtered_DF == null) {
      log.info("No records found after filter.")
      return
    }

    log.info("Performing Truncate and Load of CDS data....")
    DataUtilities.saveDataframeToDB(AppProperties.SparkSession, MIPdbProperties, filtered_DF, "MAP_STG.STG_CDS_DATA", Constants.PersistUsingSourceColumns, Constants.SaveModeOverwrite)


    log.info("Performing sql to get final joined dataframe...")
    val df: DataFrame = createFinalDF(MIPdbProperties)
    df.cache()
    val doubleQuoteStr = udf((string: String) => "\"\"" + string + "\"\"")
    val singleQuoteStr = udf((string: String) => "\"" + string + "\"")
    val removeEndStr = udf((string: String) => string.substring(0, string.length - 5))
    val replaceQuotes = udf((string: String) => string.replace("\"", "'"))

    log.info("Perform dataframe transformations for formatting...")
    val df2 = df.na.fill("")
      .filter("`entity.common-locale` == 'en-us'")
      .withColumn("entity.categoryId", doubleQuoteStr(col("`entity.categoryId`")))
      .withColumn("entity.name", replaceQuotes(col("`entity.name`")))
      .withColumn("entity.message", replaceQuotes(col("`entity.message`")))
      .withColumn("entity.name", singleQuoteStr(col("`entity.name`")))
      .withColumn("entity.message", singleQuoteStr(col("`entity.message`")))
      .withColumn("entity.pageUrl", singleQuoteStr(col("`entity.pageUrl`")))
      .withColumn("entity.ut-level10", singleQuoteStr(col("`entity.ut-level10`")))
      .withColumn("entity.ut-level15", singleQuoteStr(col("`entity.ut-level15`")))
      .withColumn("entity.ut-level17", singleQuoteStr(col("`entity.ut-level17`")))
      .withColumn("entity.ut-level20", singleQuoteStr(col("`entity.ut-level20`")))
      .withColumn("entity.ut-level30", singleQuoteStr(col("`entity.ut-level30`")))
      .withColumn("entity.content-publishedTime", removeEndStr(col("`entity.content-publishedTime`")))
      .withColumn("entity.content-modifiedTime", removeEndStr(col("`entity.content-modifiedTime`")))
      .withColumn("entity.content-publishedTime", singleQuoteStr(col("`entity.content-publishedTime`")))
      .withColumn("entity.content-modifiedTime", singleQuoteStr(col("`entity.content-modifiedTime`")))
    df2.cache()
    df2.show()


    log.info("Creating csv....")
    createCSVHeader()
    // ## RECSentity.id,entity.name,entity.category,entity.message,entity.pageUrl,entity.thumbnailUrl,entity.value,entity.inventory,entity.margin,entity.common-type,entity.common-contentId,entity.common-source,entity.common-page-categoryCode,entity.common-language,entity.common-locale,entity.common-country,entity.common-geo,entity.common-topics,entity.ut-level10code,entity.ut-level10,entity.ut-level15code,entity.ut-level15,entity.ut-level17code,entity.ut-level17,entity.ut-level20code,entity.ut-level20,entity.ut-level30code,entity.ut-level30,entity.content-publishedTime,entity.content-modifiedTime,entity.content-authorName

    df2.foreach { row =>
      writeRow(row) // For some reason it didn't like me just appending the rows here, had to create a seperate function
    }

    log.info("csv created.")
    log.info("Uploading to COS Bucket....")
    val COS_HOST = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, "COS_" + ENV) // NOSONAR
    COSUpload(COS_HOST, COS_BUCKET, "CDS_Extract_US_" + pageCategoryCode + ".csv")
    val archiveTimestamp = Calendar.getInstance().getTime.toString.replace(" ", "_").replace(":", "-")
    COSUpload(COS_HOST, COS_BUCKET + "/archives", "CDS_Extract_US_" + pageCategoryCode + "_" + archiveTimestamp + ".csv")

    if (ENV == "PROD") {
      FTPUpload(FTP_HOST, FTP_BUCKET, "CDS_Extract_US_" + pageCategoryCode + ".csv")
      FTPUpload(FTP_HOST, FTP_ARCHIVE_BUCKET, "CDS_Extract_US_" + pageCategoryCode + "_" + archiveTimestamp + ".csv")
    }

    val totalTime: Long = (System.currentTimeMillis() - startTime) / (1000 * 60)
    log.info("Total time: " + totalTime + "  minutes.")

    CDS_COS_String = new StringBuilder()

    log.info("runJobSequence ended.")
  }

  /*
   Just for writng the rows in the csv string
   */
  private def writeRow(row: Row): Unit = {
    CDS_COS_String.append(row(0) + "," + row(1) + "," + row(2) + "," + row(3) + "," + row(4) + "," + row(5) + "," + row(6) + "," + row(7) + "," + row(8) + "," + row(9) + "," + row(10) + "," + row(11) + "," + row(12) + "," + row(13) + "," + row(14) + "," + row(15) + "," + row(16) + "," + row(17) + "," + row(18) + "," + row(19) + "," + row(20) + "," + row(21) + "," + row(22) + "," + row(23) + "," + row(24) + "," + row(25) + "," + row(26) + "," + row(27) + "," + row(28) + "," + row(29) + "," + row(30) + "\n")
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
      if (responseCode == 500 || responseCode == 504) {
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
        log.info("Response code 200, parsing data")
        cdsJSON = getJSONOBJECT(urlConnection)
        return parseData(cdsJSON, extractedPagesCount)
      } else {
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

  private def convertStreamToString(stream: InputStream): String = {
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
        val coreAttributes = jsonDataArray.getJSONObject(i).get("coreAttributes").asInstanceOf[JSONObject]
        val relationshipDetailsArray = jsonDataArray.getJSONObject(i).getJSONArray("relationshipDetails")
        val universalContentId = coreAttributes.get("universalContentId")
        var contentURLId = ""
        var j = 0
        while ( {
          j < relationshipDetailsArray.length
        }) {
          val relationshipDetailsData = relationshipDetailsArray.getJSONObject(j)
          if (relationshipDetailsData.get("contentTypeId").equals("URLID")) contentURLId = contentURLId.concat(relationshipDetailsData.get("contentId").toString)

          j += 1
        }


        val contentUrl = coreAttributes.get("contentUrl")

        try {
          val url: URL = new URL("https://" + contentUrl.toString + "/")
          val huc: HttpURLConnection = url.openConnection().asInstanceOf[HttpURLConnection]
          huc.setInstanceFollowRedirects(false)
          val responseCode: Int = huc.getResponseCode()
          log.info(url + " " + responseCode)
          if (responseCode != 200) {
            log.info("Redirect detected for parsed url, skipping " + contentUrl.toString)
            skipCounter+= 1
          } else {
            log.info("Url: " + contentUrl.toString + " \nresponseCode: " + responseCode + ", adding to json")
            val contentSourceSystem = coreAttributes.get("contentSourceSystem")
            val pageCatCd = coreAttributes.get("pageCatCd")
            val langCd = coreAttributes.get("langCd")
            val ctryCd = coreAttributes.get("ctryCd")
            val geoCd = coreAttributes.get("geoCd")
            val ut10Cd = coreAttributes.get("ut10Cd")
            val ut15Cd = coreAttributes.get("ut15Cd")
            val ut17Cd = coreAttributes.get("ut17Cd")
            val ut20Cd = coreAttributes.get("ut20Cd")
            val ut30Cd = coreAttributes.get("ut30Cd")
            val contentPublshdTs = coreAttributes.get("contentPublshdTs")
            val contentUpdtTs = coreAttributes.get("contentUpdtTs")
            val contentOwnerName = coreAttributes.get("contentOwnerName")
            val contentTitle = coreAttributes.get("contentTitle")
            val contentDscr = coreAttributes.get("contentDscr")

            /** entity.id , entity.page-Url , entity.common-content-Id , entity.common-source , entity.common-page-Category-Code , entity.common-language , entity.common-country , entity.common-geo , entity.ut-level_10_code , entity.ut-level_15_code , entity.ut-level_17_code , entity.ut-level_20_code , entity.ut-level_30_code
             * , entity.content-published-Time , entity.content-modified-Time , entity.content-author-name */

            val JsonValues = new JSONObject().put("URLID", contentURLId)
              .put("PAGE_URL", contentUrl)
              .put("CONTENT_ID", universalContentId)
              .put("SOURCE", contentSourceSystem)
              .put("PAGECATCODE", pageCatCd)
              .put("LANG_CD", langCd)
              .put("CNTRY", ctryCd)
              .put("GEO", geoCd)
              .put("UT10", ut10Cd)
              .put("UT15", ut15Cd)
              .put("UT17", ut17Cd)
              .put("UT20", ut20Cd)
              .put("UT30", ut30Cd)
              .put("PUBLISHEDTIME", contentPublshdTs)
              .put("MODIFIEDTIME", contentUpdtTs)
              .put("AUTHORNAME", contentOwnerName)
              .put("CONTENT_TITLE", contentTitle)
              .put("CONTENT_DESCRIPTION", contentDscr)

            cdsJSONFull.put(JsonValues)
          }
        } catch {
          case e: Exception =>
            log.info("Exception found with url: " + contentUrl.toString + "\nPossible redirect or malformed url, skipping...")
            skipCounter+= 1
        }
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

  def jsonToDataFrame(jsonString: String): DataFrame = {
    val spark = AppProperties.SparkSession
    import spark.implicits._
    val df = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(jsonString)).toDS())
    df
  }

  def createFinalDF(dbProperties: Properties): DataFrame = {

    val getsqltext = s"SELECT SQL_TEXT, 4 as PARTCOL " +
      s"FROM $ETL_SCHEMA.ETL_JOB_SQL " +
      s"WHERE SQL_ID IN ('ADOBE_TARGET_CSV')"

    val sqlDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, dbProperties, getsqltext, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    val mergedDFsql = sqlDF.first().get(0).toString
    log.info("Collecting Adobe Target CSV sql..")
    if (mergedDFsql == null) {
      throw new IllegalArgumentException(
        "Could not find Adobe Target CSV sql."
      )
    }
    log.info("Sql found: \n" + mergedDFsql)
    val mergedDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, dbProperties, mergedDFsql, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    mergedDF
  }

  def createCSVHeader() {
    CDS_COS_String.append("## RECSRecommendations Upload File" + "\n")
    CDS_COS_String.append("## RECS''## RECS'' indicates a Recommendations pre-process header. Please do not remove these lines." + "\n")
    CDS_COS_String.append("## RECS" + "\n")
    CDS_COS_String.append("## RECSUse this file to upload product display information to Recommendations. Each product has its own row. Each line must contain 19 values and if not all are filled a space should be left." + "\n")
    CDS_COS_String.append("## RECSThe last 100 columns (entity.custom1 - entity.custom100) are custom. The name 'customN' can be replaced with a custom name such as 'onSale' or 'brand'." + "\n")
    CDS_COS_String.append("## RECSIf the products already exist in Recommendations then changes uploaded here will override the data in Recommendations. Any new attributes entered here will be added to the product''s entry in Recommendations." + "\n")
    CDS_COS_String.append("## RECSentity.id,entity.name,entity.categoryId,entity.message,entity.thumbnailUrl,entity.value,entity.pageUrl,entity.inventory,entity.margin,entity.common-type,entity.common-contentId,entity.common-source,entity.common-page-categoryCode,entity.common-language,entity.common-locale,entity.common-country,entity.common-geo,entity.common-topics,entity.ut-level10code,entity.ut-level10,entity.ut-level15code,entity.ut-level15,entity.ut-level17code,entity.ut-level17,entity.ut-level20code,entity.ut-level20,entity.ut-level30code,entity.ut-level30,entity.content-publishedTime,entity.content-modifiedTime,entity.content-authorName" + "\n")

  }

  private def COSUpload(connection: Properties, directory: String, filename: String): Int = { // NOSONAR
    val bucketURL = connection.getProperty(PropertyNames.EndPoint) + "/" +
      connection.getProperty(PropertyNames.ResourceSpecific_1) + "/" + directory + "/" + filename
    log.info("Uploading to COS url: " + bucketURL)
    val url = new URL(bucketURL)
    val conn = url.openConnection.asInstanceOf[HttpURLConnection]
    conn.setDoOutput(true)
    conn.setRequestMethod("PUT")
    conn.setRequestProperty("Authorization", getToken())
    val os = conn.getOutputStream
    os.write(CDS_COS_String.toString().getBytes)
    os.flush()
    os.close()
    val responseCode = conn.getResponseCode
    conn.disconnect()
    log.info("Upload Response code: " + responseCode)
    responseCode
  }

  private def getToken(): String = {
    log.info("Getting COS access Token....")
    var accessToken: String = ""
    try { //Get API Key of the Service ID (Service Role) from Env Variables
      val accessKey = System.getenv("OW_IAM_NAMESPACE_API_KEY")
      val iamUrl = DEFAULT_IAM_ENDPOINT + accessKey
      val accessTempToken = new StringBuilder
      var keyToken = ""
      val urlGetToken = new URL(iamUrl)
      val connGetToken = urlGetToken.openConnection.asInstanceOf[HttpURLConnection]
      connGetToken.setRequestMethod("POST")
      connGetToken.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
      connGetToken.setRequestProperty("Accept", "application/json")
      val buffReader = new BufferedReader(new InputStreamReader(connGetToken.getInputStream))
      var line: String = buffReader.readLine()
      var i = 0
      while ( {
        line  != null
      }) {
        i += 1
        accessTempToken.append(line)
        line = buffReader.readLine()
      }
      buffReader.close()
      connGetToken.disconnect()
      val token = new JsonParser().parse(accessTempToken.toString).getAsJsonObject
      if (token.get("access_token") != null && !token.get("access_token").isJsonNull) keyToken = token.get("access_token").getAsString
      accessToken= "Bearer " + keyToken
    } catch {
      case ex: Exception =>
        throw new Exception("Unable to retrieve access token")
    }
    accessToken
  }

  private def FTPUpload(host: String, directory: String, filename: String): Unit = { // NOSONAR

    val user = System.getenv("MAP_FTP_USER_ID")
    val pass = System.getenv("MAP_FTP_PASSWORD")

    val input: InputStream = new ByteArrayInputStream(CDS_COS_String.toString().getBytes())

    var response = false
    try {
      val fclient = new SSLSessionReuseFTPSClient
      log.info("Connecting to FTP....")
      fclient.connect(host)
      log.info("Logging into FTP Server....")
      if (!fclient.login(user, pass)) {
        log.info("Failed to login..")
        log.info(fclient.getReplyString)
        fclient.logout
        return false
      }
      val reply = fclient.getReplyCode
      if (!FTPReply.isPositiveCompletion(reply)) {
        fclient.disconnect()
        return false
      }
      fclient.execPBSZ(0) // Set protection buffer size
      fclient.execPROT("P") // Set data channel protection to private
      fclient.setFileType(FTP.BINARY_FILE_TYPE, FTP.BINARY_FILE_TYPE)
      fclient.setFileTransferMode(FTP.BINARY_FILE_TYPE)
      fclient.enterLocalPassiveMode()
      fclient.setKeepAlive(true)


      val cd = fclient.changeWorkingDirectory(directory)
      if (cd) {
        log.info("Changed success to " + directory)
      }
      log.info("Uploading....")
      response = fclient.storeFile(filename, input)
      val re: String = fclient.getReplyString
      log.info(response + ": :" + re)
      if (response) {
        log.info("File uploaded successfully at " + directory + "/" + filename)
      } else {
        log.info("File failed to upload")
      }


      input.close()
      fclient.logout
      fclient.disconnect()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        return false
    }
    return response


  }

  class SSLSessionReuseFTPSClient extends FTPSClient { // adapted from: https://trac.cyberduck.io/changeset/10760
    @throws[IOException]
    override protected def _prepareDataSocket_(socket: Socket): Unit = { // NOSONAR
      if (socket.isInstanceOf[SSLSocket]) {
        val session = _socket_.asInstanceOf[SSLSocket].getSession
        val context = session.getSessionContext
        try {
          val sessionHostPortCache = context.getClass.getDeclaredField("sessionHostPortCache")
          sessionHostPortCache.setAccessible(true)
          val cache = sessionHostPortCache.get(context)
          val putMethod = cache.getClass.getDeclaredMethod("put", classOf[Any], classOf[Any])
          putMethod.setAccessible(true)
          val getHostMethod = socket.getClass.getDeclaredMethod("getHost")
          getHostMethod.setAccessible(true)
          val host = getHostMethod.invoke(socket)
          val key = String.format("%s:%s", host, String.valueOf(socket.getPort)).toLowerCase(Locale.ROOT)
          putMethod.invoke(cache, key, session)
        } catch {
          case e: Exception =>
            throw Throwables.propagate(e)
        }
      }
    }
  }

  private def getLastRun(MIPdbProperties: Properties): Unit = {
    val lastRunSql = s"SELECT MAX(JOB_END_TIME) AS LASTPROCESSTIME, 4 as PARTCOL " +
      s"FROM $ETL_SCHEMA.ETL_JOB_HIST " +
      s"WHERE JOB_SEQUENCE = '$jobClassName' " +
      s"AND JOB_STATUS = 'Succeeded'"

    val timeDF = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, MIPdbProperties, lastRunSql, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    val timestamp = timeDF.first().get(0)
    if (timestamp != null) {
      lastRunTimestamp = timestamp.toString
    }
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
      timestamp = Timestamp.from(instant.minus(MINIMUM_TIMESTAMP_OFFSET, ChronoUnit.DAYS))
    } else if (mode == "max") {
      // 1 day future in order to make sure time range goes all the way to current as leeway
      timestamp = Timestamp.from(Instant.now().plus(1, ChronoUnit.DAYS).minus(MAXIMUM_TIMESTAMP_OFFSET, ChronoUnit.DAYS))
    }
    println(timestamp)
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

    val baseDBIndex = args.indexOf("--baseDB")
    val defaultRunIndex = args.indexOf("--defaultRunTime")

    if (args.length % 2 == 0) {
      ENV = args(args.indexOf("--ENV") + 1)
      COS_BUCKET = args(args.indexOf("--cosFolder") + 1)
      MINIMUM_TIMESTAMP_OFFSET = args(args.indexOf("--minOffset") + 1).toInt
      MAXIMUM_TIMESTAMP_OFFSET = args(args.indexOf("--maxOffset") + 1).toInt
    }

    if (baseDBIndex >= 0) {
      log.info(s"Arg 'baseDB' found at index: $baseDBIndex")
      if (args.length > (baseDBIndex + 1)) {
        val baseDB = args(baseDBIndex + 1)
        log.info(s"Value for arg 'baseDB' value: $baseDB")
        MIP_ENDPOINT = baseDB
      } else {
        throw new IllegalArgumentException(
          "Value for arg 'baseDB' could not be found. Please pass the value."
        )
      }
    }

    if (defaultRunIndex >= 0) {
      log.info(s"Arg 'defaultRunTimestamp' found at index: $defaultRunIndex")
      if (args.length > (defaultRunIndex + 1)) {
        val defaultRun = args(defaultRunIndex + 1)
        log.info(s"Value for arg 'defaultRun' value: $defaultRun")
        defaultMinTimestamp = defaultRun
      } else {
        throw new IllegalArgumentException(
          "Value for arg 'defaultRun' could not be found. Please pass the value."
        )
      }
    }
    val cds_details = args(args.indexOf("--cdsDetails") +1) // NOSONAR
    val cds_conn = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, cds_details) // NOSONAR
    cdsEndPoint = cds_conn.getProperty(PropertyNames.EndPoint)
    apiKey = cds_conn.getProperty(PropertyNames.ClientSecret)
  }

  def main(args: Array[String]): Unit = {
    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      // Log job status START - DB
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobStarted,
        s"$jobClassName Started", null, null)


      log.info("Starting ETL...")
      getArgs(args)
      val MIPdbProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, MIP_ENDPOINT)
      MIPdbProperties.setProperty("sslConnection", "true")
      getLastRun(MIPdbProperties)
      runJobSequence_getCDSAttributes(pageCategoryCode = USProductCode, MIPdbProperties)
      val USProductCount = extractedPagesCount
      extractedPagesCount = 0
      countSoFar = 0
      val USPRoductSkip = skipCounter
      skipCounter = 0
      runJobSequence_getCDSAttributes(pageCategoryCode = USBlogCode, MIPdbProperties)
      val USBlogCount = extractedPagesCount
      extractedPagesCount = 0
      countSoFar = 0
      val USBlogSkip = skipCounter
      skipCounter = 0
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
        s"Completed Job => $jobClassName.  $USProductCount extracted for $USProductCode with $USPRoductSkip skipped, $USBlogCount extracted for $USBlogCode with $USBlogSkip skipped.", null, null)

      log.info(s"Completed Job => $jobClassName.")

    } catch {
      case e: Throwable =>
        isJobFailed = true
        e.printStackTrace()
        log.error(e.getMessage + " - " + e.getCause)
        DataUtilities.recordJobHistory(AppProperties.SparkSession,
          AppProperties.CommonJobSeqCode,
          0,
          Constants.JobFailed,
          e.getMessage + " - " + e.getCause, null, null)
    } finally {
      this.cleanUpFramework(AppProperties.SparkSession)
      log.info(s"Exiting Job => $jobClassName...")

      if (isJobFailed) {
        System.exit(1)
      }
    }
  }

}
