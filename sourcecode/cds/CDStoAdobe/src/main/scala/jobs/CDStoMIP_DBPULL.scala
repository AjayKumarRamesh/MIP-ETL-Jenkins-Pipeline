package jobs

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.json.{JSONArray, JSONException, JSONObject}
import java.io.{IOException, _}
import java.net.URL
import java.util._

import com.ibm.mkt.etlframework.data.jdbc.TableCDCDetails
import javax.net.ssl.HttpsURLConnection


object CDStoMIP_DBPULL extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var isJobFailed = false

  // Static values for pathing to FTP server
  //private val FTP_HOST = "w3-transfer.boulder.ibm.com"
  //private val FTP_BUCKET = "/www/prot/map_etl"
  //private val FTP_ARCHIVE_BUCKET = "/www/prot/map_etl/archive"

  // Static values for creating api call
  //private var pageCatCd: String = null
  //private val ctryCd: String = "US"
  private var extractedPagesCount: Int = 0
  var countSoFar: Long = 0


  // Will hold all the Json to create the array in the end
  private var cdsJSONFull: JSONArray = new JSONArray()

  // Database Endpoint variables
  private var MIP_ENDPOINT = ""
  private var CMDP_ENDPOINT = ""
  private var CORE_SCHEMA = ""
  private var CDS_ASSET_TABLE = ""

  // Argument Variables
  private var mergeSql = ""
  private var cmdpPullSql = ""



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
  def runJobSequence_CDStoMIP(): Unit = {
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

    //pageCatCd = pageCategoryCode


    // Built api call
    //val cdsURL = cdsEndPoint + "?apiKey=" + apiKey + "&page=" + page + "&size=" + size + "&source=" + source

    // For tracking runtime of job
    val startTime = System.currentTimeMillis()
    val startDate = new Date(startTime * 1000L)
    log.info("start time: " + startDate)

    val MIPdbProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession,MIP_ENDPOINT)
    MIPdbProperties.setProperty("sslConnection", "true")
    val cmdpProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, CMDP_ENDPOINT)

    //println(MIPdbProperties.stringPropertyNames())
    //println(MIPdbProperties.setProperty("sslConnection", "true"))
    //println(MIPdbProperties.values())

    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 2000

    log.info("Pulling CMDP data.....")

    //val df_unord = DataUtilities.readData(AppProperties.SparkSession, cmdpProperties, cmdpPullSql, null)
    val df_unord = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, cmdpProperties, cmdpPullSql, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    df_unord.cache()

    val spark = AppProperties.SparkSession
    import spark.implicits._

    // ("036c73f5d083d64e",null,"LogoCampaign_OSS_Contactform","www.ibm.com/account/reg/br-pt/signup?formid=urx-49009","https://www.ibm.com/account/reg/br-pt/signup?formid=urx-49009","www.ibm.com/account/reg/br-pt/signup?formid=urx-49009",null,"10G00","15TSS","17LGS","20G20","30GLS","pt","BRTT")

    //val values = Seq(("036c73f5d083d64d",null,"LogoCampaign_OSS_Contactform","www.ibm.com/account/reg/br-pt/signup?formid=urx-49009","https://www.ibm.com/account/reg/br-pt/signup?formid=urx-49009","www.ibm.com/account/reg/br-pt/signup?formid=urx-49009",null,"10G00","15TSS","17LGS","20G20","30GLS","pt","BR"))
    //val testDF = values.toDF("UUC_ID", "OV_CODE", "ASSET_DEFAULT_TITLE", "CONTENT_URL","DLVRY_URL","DLVRY_URL_ID", "CONTENT_TYPE_ID" ,"UT10_CODE","UT15_CODE","UT17_CODE","UT20_CODE","UT30_CODE","LANG_CODE","COUNTRY_CODE")



      log.info("Perform dataframe transformations for formatting...")
    // Drop any columns with null UUC_ID values (before turning those values to "").
    val df = df_unord.na.drop(Seq("UUC_ID"))
              .select("UUC_ID","ASSET_DEFAULT_TITLE","CONTENT_URL","DLVRY_URL","DLVRY_URL_ID","UT10_CODE","UT15_CODE","UT17_CODE","UT20_CODE","UT30_CODE","COUNTRY_CODE","LANG_CODE","CONTENT_TYPE_ID","OV_CODE")
    df.cache()


    df.show()

    countSoFar = df.count()

    //log.info("Performing Truncate and Load of CDS data....")
    //DataUtilities.saveDataframeToDB(AppProperties.SparkSession, MIPdbProperties, df, "MAP_CORE.MCT_CDS_ASSET",Constants.PersistUsingSourceColumns, Constants.SaveModeOverwrite)
    log.info("Performing Update/Insert of CDS data....")

    DataUtilities.runPreparedStatement(
      MIPdbProperties,
      df,
      mergeSql,
      df.columns,
      null,
      null,
      null)

    //DataUtilities.performMergeTablesUsingInsertAndUpdate()

    //var crudMask = Constants.CDCActionMaskInsert | Constants.CDCActionMaskUpdate | Constants.CDCActionMaskBuildSuccessRecsLogs




    val totalTime: Long = (System.currentTimeMillis()-startTime)/(1000*60)

    log.info("Total time: "  + totalTime + "  minutes.")

    log.info("runJobSequence ended.")
  }

  def jsonToDataFrame(jsonString: String):DataFrame ={
    val spark = AppProperties.SparkSession
    import spark.implicits._
    val df = AppProperties.SparkSession.read.json(AppProperties.SparkSession.sparkContext.parallelize(Seq(jsonString)).toDS())
    df
  }

  /*
 Searches the command line arguments for the baseDB argument and defaultruntimestamp.
 Looks for "--baseDB" and then assumes the MIP database argument will be the index after.
 Needed for knowing which MIP database to put in for the DataUtilities connection details.
 Similarly looks for index for default runtimestmap "--defaultRunTime".
 */
  private def getArgs(args: Array[String]): Unit = {

    //val baseDBIndex = args.indexOf("--baseDB")
    //val cmdpDBIndex = args.indexOf("--cmdpDB")

    if (args.length % 2 == 0) {
      MIP_ENDPOINT = args(args.indexOf("--baseDB") + 1)
      CORE_SCHEMA = args(args.indexOf("--trgtSchema") + 1)
      CDS_ASSET_TABLE = args(args.indexOf("--trgtTable") + 1)
      CMDP_ENDPOINT = args(args.indexOf("--cmdpDB") + 1)
      mergeSql = args(args.indexOf("--mergeSql") +1)
      cmdpPullSql = args(args.indexOf("--cmdpPullSql") +1)
    }
  }

  def main(args: Array[String]): Unit = {
    try {

      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")

      log.info(s"Starting ETL Job => $jobClassName....")

      // Log job status START - DB
      // log.info(s"CommonDBConnProperties => ${this.CommonDBConProperties}")
      // log.info(s"Log to JobHistoryLogTable => ${AppProperties.JobHistoryLogTable}")
      DataUtilities.recordJobHistory(AppProperties.SparkSession,
        AppProperties.CommonJobSeqCode,
        0,
        Constants.JobStarted,
        "CDSExtract Started",null,null)


      log.info("Starting ETL...")

      getArgs(args)
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

