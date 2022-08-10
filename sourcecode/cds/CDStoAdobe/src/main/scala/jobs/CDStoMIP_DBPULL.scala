package jobs

import java.util._

import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}
import org.apache.spark.sql.DataFrame


object CDStoMIP_DBPULL extends ETLFrameWork {
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  var isJobFailed = false

  // Static values for creating api call
  var countSoFar: Long = 0
  

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

    
    // For tracking runtime of job
    val startTime = System.currentTimeMillis()
    val startDate = new Date(startTime * 1000L)
    log.info("start time: " + startDate)

    val MIPdbProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession,MIP_ENDPOINT)
    MIPdbProperties.setProperty("sslConnection", "true")
    val cmdpProperties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, CMDP_ENDPOINT)

    AppProperties.DefaultPartitionCount = 1
    AppProperties.DefaultWriteBatchSize = 2000

    log.info("Pulling CMDP data.....")

    val df_unord = DataUtilities.readDataByPartitioningType(AppProperties.SparkSession, cmdpProperties, cmdpPullSql, Constants.PartitionTypeByColumn, null, "PARTCOL").drop("PARTCOL")
    df_unord.cache()

    val spark = AppProperties.SparkSession



      log.info("Perform dataframe transformations for formatting...")
    // Drop any columns with null UUC_ID values (before turning those values to "").
    val df = df_unord.na.drop(Seq("UUC_ID"))
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

