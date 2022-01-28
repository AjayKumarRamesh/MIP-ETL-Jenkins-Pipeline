package SampleJobs.adhocdata

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Properties

import com.ibm.mkt.etlframework.audit.JobRunArgs
import com.ibm.mkt.etlframework.data.DataUtilities
import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork, PropertyNames}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._



object adhocdata extends ETLFrameWork {

  var jobSeq: String = null
  var targetdb: String = null
  var sftpSourcepath: String = null
  var sftpSource: String = null
  val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
  //var recordcount: Int = 0

  var bException: Boolean = false

  def main(args: Array[String]): Unit = {
    val jobRunArgs: JobRunArgs = new JobRunArgs
    var i = 0
    jobSeq = args(args.indexOf("--jobseq") + 1)
    targetdb = args(args.indexOf("--targetdb") + 1)
    sftpSource = args(args.indexOf("--sftpSource") + 1)


    try {
      //Initialization of Variables
      log.info("Initialization started")
      this.initializeFramework(args)
      log.info("Initialization completed.")
      log.info("Executing ETL logic...")

      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSeqCode, Constants.JobStarted)

      //connection properties to MIP DB
      val conProp1: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, targetdb)
      val dbConnectionInfo = conProp1.getProperty(PropertyNames.EndPoint)

      //Extracting timestamp of last successful job run to compare it against files within person response directory to extract latest files.
      val dfresult1 = getLastSuccessfulRunTime(AppProperties.SparkSession, "adhoc_data", null)
      val format = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss")
      var dfresult2: String = null

      for ((key, value) <- dfresult1) {
        println(s"$key : $value")
        dfresult2 = (value)
        println(dfresult2 + "dfresult2 ")
      }
      //converting dfresult2 from string to date format
      val rundate = format.parse(dfresult2)

      //connection properties for SFTP server
      val conProp2: Properties = DataUtilities.getDataSourceDetails(AppProperties.SparkSession, sftpSource)
      println(conProp2)

      //retrieving stored path from etl_data_sources table path belongs to sftp person response file
      val path = conProp2.getProperty(PropertyNames.UserPassword)
      print("path is" + path )
      //Reading latest files and converting it into dataframe based on date in rundate (picking files created only after date)
      val files = SFTPinJava.getFilesToParseV2(conProp2, path, "person_response/", "person-response-", "csv", rundate)

      //to run one time job where we are ingesting data from gary's process
      //val files = SFTPinJava.getFilesToParseV2(conProp2, path, "person_response/", "mktoDB20210908", "csv", rundate)

      if (files != null && !files.isEmpty) {
        files.show(false)
         val count = files.count()
        //recordcount = count.toInt

        //creating dataframe with missing columns and values
        val postDF = files.withColumn("MIP_TRANSACTION_ID", lit(-1))
          .withColumn("CREATE_TS", current_timestamp())
          .withColumn("UPDATE_TS", current_timestamp())
          .withColumn("CREATE_USER", lit("CMDP"))
          .withColumn("PROCESSED_FLAG", lit("Y"))
          .withColumn("PROCESSED_TS", current_timestamp())
          .withColumn("TRANSACTION_TS", current_timestamp())
        postDF.show()

        //extracting desired column from postdf dataframe
        var df2 = postDF.select("MIP_TRANSACTION_ID", "marketo_id", "EMAIL_ADR", "URN_IDM_INDIV", "INDIV_EMAIL_MEDIA_ID", "CREATE_TS", "UPDATE_TS", "CREATE_USER", "PROCESSED_FLAG", "PROCESSED_TS", "TRANSACTION_TS")

        //to run one time job where we are ingesting data from gary's process
        //var df2 = postDF.select("MIP_TRANSACTION_ID", "id", "email", "IDM_ID", "Email_Media_ID", "CREATE_TS", "UPDATE_TS", "CREATE_USER", "PROCESSED_FLAG", "PROCESSED_TS", "TRANSACTION_TS")

        df2.show()

        //reading data from the file and changing names of column
        df2 = df2.withColumnRenamed("MIP_TRANSACTION_ID", newName = "MIP_SEQ_ID")
          .withColumnRenamed("id", newName = "MKTO_LEAD_ID")
          .withColumnRenamed("email", newName = "EMAIL_ADDR")
          .withColumnRenamed("IDM_ID", newName = "IDM_ID")
          .withColumnRenamed("Email_Media_ID", newName = "EMAIL_MEDIA_ID")
          .withColumnRenamed("CREATE_TS", newName = "CREATE_TS")
          .withColumnRenamed("UPDATE_TS", newName = "UPDATE_TS")
          .withColumnRenamed("CREATE_USER", newName = "CREATE_USER")
          .withColumnRenamed("PROCESSED_FLAG", newName = "PROCESSED_FLG")
          .withColumnRenamed("PROCESSED_TS", newName = "PROCESSED_TS")
          .withColumnRenamed("TRANSACTION_TS", newName = "TRANSACTION_TS").persist()
          //df2.printSchema()

           //column mapping
          val colmapping =
          s"""MIP_SEQ_ID=MIP_SEQ_ID,
             |MKTO_LEAD_ID=MKTO_LEAD_ID,
             |EMAIL_ADDR=EMAIL_ADDR,
             |IDM_ID=IDM_ID,
             |EMAIL_MEDIA_ID=EMAIL_MEDIA_ID,
             |PROCESSED_FLG=PROCESSED_FLG,
             |PROCESSED_TS=PROCESSED_TS,
             |TRANSACTION_TS=TRANSACTION_TS,
             |CREATE_TS=CREATE_TS,
             |UPDATE_TS=UPDATE_TS,
             |CREATE_USER=CREATE_USER""".stripMargin.replaceAll("\\s+", "")


          //Insert statement to get data ingested from dataframe to table in mip_dev db (MAP_MKTO.MCT_MKTO_LEAD_XREF)
          //DataUtilities.performInsertRecords(conProp1, df2, "MAP_MKTO.MCT_MKTO_LEAD_XREF", Array( "MIP_SEQ_ID", "MKTO_LEAD_ID", "EMAIL_ADDR", "IDM_ID", "EMAIL_MEDIA_ID", "PROCESSED_FLG", "PROCESSED_TS", "TRANSACTION_TS", "CREATE_TS", "UPDATE_TS", "CREATE_USER"), colmapping, false)




        DataUtilities.runPreparedStatement(conProp1,df2,s"MERGE INTO MAP_MKTO.MCT_MKTO_LEAD_XREF mmlx" +
          s"\nUSING ( VALUES (?,?,?,?,?,?,?,?,?,?,?)) " +
          s"\nAS df2 ( MIP_SEQ_ID, MKTO_LEAD_ID, EMAIL_ADDR, IDM_ID, EMAIL_MEDIA_ID, PROCESSED_FLG, PROCESSED_TS, TRANSACTION_TS, CREATE_TS, UPDATE_TS, CREATE_USER)" +
          s"\nON mmlx.MKTO_LEAD_ID = df2.MKTO_LEAD_ID" +
          s"\n\tWHEN MATCHED THEN UPDATE SET " +
          s"\n\t mmlx.MIP_SEQ_ID = df2.MIP_SEQ_ID,"+
          s"\n\t mmlx.MKTO_LEAD_ID = df2.MKTO_LEAD_ID,"+
          s"\n\t mmlx.EMAIL_ADDR = df2.EMAIL_ADDR,"+
          s"\n\t mmlx.IDM_ID = df2.IDM_ID,"+
          s"\n\t mmlx.EMAIL_MEDIA_ID = df2.EMAIL_MEDIA_ID,"+
          s"\n\t mmlx.PROCESSED_FLG = df2.PROCESSED_FLG,"+
          s"\n\t mmlx.PROCESSED_TS = df2.PROCESSED_TS,"+
          s"\n\t mmlx.TRANSACTION_TS = df2.TRANSACTION_TS,"+
          s"\n\t mmlx.CREATE_TS = df2.CREATE_TS,"+
          s"\n\t mmlx.UPDATE_TS = df2.UPDATE_TS,"+
          s"\n\t mmlx.CREATE_USER = df2.CREATE_USER"+
          s"\n\tWHEN NOT MATCHED THEN INSERT(MIP_SEQ_ID, MKTO_LEAD_ID, EMAIL_ADDR, IDM_ID, EMAIL_MEDIA_ID, PROCESSED_FLG, PROCESSED_TS, TRANSACTION_TS, CREATE_TS, UPDATE_TS, CREATE_USER)" +
          s"\n\tVALUES(df2.MIP_SEQ_ID,df2.MKTO_LEAD_ID,df2.EMAIL_ADDR,df2.IDM_ID,df2.EMAIL_MEDIA_ID,df2.PROCESSED_FLG,df2.PROCESSED_TS,df2.TRANSACTION_TS,df2.CREATE_TS,df2.UPDATE_TS,df2.CREATE_USER)",
          Array( "MIP_SEQ_ID", "MKTO_LEAD_ID", "EMAIL_ADDR", "IDM_ID", "EMAIL_MEDIA_ID", "PROCESSED_FLG", "PROCESSED_TS", "TRANSACTION_TS", "CREATE_TS", "UPDATE_TS", "CREATE_USER"),null,"MIP_SEQ_ID=MIP_SEQ_ID,MKTO_LEAD_ID=marketo_id,EMAIL_ADDR=EMAIL_ADR,IDM_ID=URN_IDM_INDIV,EMAIL_MEDIA_ID=INDIV_EMAIL_MEDIA_ID,PROCESSED_FLG=PROCESSED_FLG,PROCESSED_TS=PROCESSED_TS,TRANSACTION_TS=TRANSACTION_TS,CREATE_TS=CREATE_TS,UPDATE_TS=UPDATE_TS,CREATE_USER=CREATE_USER")

      } else (
        print("no data or files left to ingest")
        )
      } catch
      {
        case e: Throwable =>
          e.printStackTrace()
          log.error(e.getMessage + " - " + e.getCause)
          bException = true
          jobRunArgs.comments = e.getMessage + " - " + e.getCause
      }
      finally
      {
        if (bException) { // Job failed
          DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSeqCode, Constants.JobFailed, jobRunArgs)

        } else {
          DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, AppProperties.CommonJobSeqCode, Constants.JobSucceeded, jobRunArgs)

        }
        this.cleanUpFramework(AppProperties.SparkSession)
        log.info(s"Exiting Job => $jobClassName...")
        if (bException) {
          System.exit(1)
        }
      }
    }

        //method to get last successful job run timestamp
        def getLastSuccessfulRunTime(sparkSession: SparkSession, jobSeqCode: String, jobSubSeqCode: String = null): Map[String, String] = {
        var mapReturn = Map[String, String]()
          var dfResult: DataFrame = null
          val jobSebSeqCondition = if (jobSubSeqCode != null && jobSubSeqCode.nonEmpty) " AND SUB_SEQ_CODE = $jobSubSeqCode" else ""
          try {
          val jobSeqHistTable: String = AppProperties.JobHistoryLogTable
            dfResult = AppProperties.SparkSession.read.jdbc(AppProperties.CommonDBConProperties.getProperty(PropertyNames.EndPoint),
            s""" (SELECT
	                    JOB_SK JOB_SK, VARCHAR_FORMAT(JOB_START_TIME,'YYYY-MM-DD-HH24.MI.SS') AS JOB_START_TIME, VARCHAR_FORMAT(JOB_END_TIME,'YYYY-MM-DD-HH24.MI.SS') AS JOB_END_TIME
	                  FROM $jobSeqHistTable
                    WHERE
	                    SEQ_CODE = '$jobSeqCode' AND JOB_STATUS = '${Constants.JobStatusSucceeded}'
                      $jobSebSeqCondition
                    ORDER BY JOB_SK DESC
                    FETCH FIRST ROW ONLY) AS RESULT_TABLE""",
              AppProperties.CommonDBConProperties)
              if (log.isDebugEnabled || log.isInfoEnabled()) dfResult.show()
              if (dfResult.count() == 0) {
              //mapReturn += (Constants.JobStartTime -> "1900-01-01-00.00.00")
              mapReturn += (Constants.JobEndTime -> "2021-07-15-13.53.50.215385")
            } else {
          val firstRow = dfResult.collect().head
          //mapReturn += (Constants.JobStartTime -> firstRow.getAs[String](Constants.JobStartTime))
          mapReturn += (Constants.JobEndTime -> firstRow.getAs[String](Constants.JobEndTime))
        }
      } finally {
        if (dfResult != null) dfResult.unpersist()
      }
      mapReturn
    }
}


