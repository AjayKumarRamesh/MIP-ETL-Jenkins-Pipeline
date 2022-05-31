import com.ibm.mkt.etlframework.{AppProperties, Constants, ETLFrameWork}
import com.ibm.mkt.etlframework.data.DataUtilities

import java.util.Properties
import scala.sys.exit
import scala.sys.process._

object spssTrigger extends ETLFrameWork {

  var keyStore: String = null
  var spssJar: String = null
  var configProp: String = null
  var outputFilePath: String = null
  var bException: Boolean = false


  def main(args: Array[String]): Unit =
  {
    log.info("Initialization started")
    this.initializeFramework(args)
    log.info("Initialization completed.")
    val jobClassName: String = this.getClass.getSimpleName.stripSuffix("$")
    log.info(s"Starting ETL Job => $jobClassName....")

    keyStore = args(args.indexOf("--keyStore") + 1)
    spssJar = args(args.indexOf("--spssJar") + 1)
    configProp = args(args.indexOf("--configProp") + 1)
    outputFilePath = args(args.indexOf("--outputFilePath") + 1)

    DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "",
      Constants.JobStarted)
    val test_d = s"java -Djavax.net.ssl.trustStore=$keyStore -jar $spssJar $configProp".!
    println("Process exit code: "+test_d)

    try {

      if (test_d == 0) {
        log.info("Job completed successfully")
        val dirContents = "ls".!!
        println(dirContents)
        val source = scala.io.Source.fromFile("spssloging.txt")
        val lines = try source.mkString finally source.close()
        log.info(lines)
        if (lines.contains("Run completed Successfully: ENDED")) {
          log.info("No errors found")
          val rm_file_if = s"rm spssloging.txt".!
        }
        else {
          log.info("Errors found")
          val rm_file_else = s"rm spssloging.txt".!
          bException = true
        }
      }
      else {
        if (test_d == 255)
          log.info("*** Error code 255 is SPSS network connection issue. Please contact SPSS team ***")
        log.info("Job failed")
        bException = true
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
      DataUtilities.recordJobHistory(AppProperties.SparkSession, AppProperties.CommonJobSeqCode, "",
        if (bException) Constants.JobFailed else Constants.JobSucceeded)
      log.info("Closing db connections")
      this.cleanUpFramework(AppProperties.SparkSession)
      if(bException)
      {
        log.info(s"Exiting Job => $jobClassName...")
        exit(1)
      }
      else
      {
        log.info(s"Exiting Job => $jobClassName...")
        exit(0)
      }
    }
  }
}