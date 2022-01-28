

package SampleJobs.adhocdata

import java.util.Properties

import com.ibm.mkt.etlframework.exception.DataWriterException
import com.ibm.mkt.etlframework.{AppProperties, Constants, Logging, PropertyNames}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SFTPUtil extends Logging {

  final val AuthTypePasswordString: Int = 101
  final val AuthTypePasswordFile: Int = 102

  @throws(classOf[Exception])
  def uploadFiles(connProperties: Properties, fileList: List[String]) = {

  }

  def getAuthTypeAsString(authType: Int): String = {
    if (authType == AuthTypePasswordFile) "pem"
    else "password"
  }

  def sftpWriteCsvFile(sparkSession: SparkSession, conProperties: Properties, dfCsv: org.apache.spark.sql.DataFrame,
                       filePath:String, fileExtension: String, authType: Int): Boolean = {
    var bRet:Boolean = true
    log.info("AppProperties.CsvDelimiter => "+AppProperties.CsvDelimiter)
    try {
      val authTypeVal: String = getAuthTypeAsString(authType)

      dfCsv.write.
        format("com.springml.spark.sftp").
        option("host", conProperties.getProperty(PropertyNames.EndPoint)).
        option("username",conProperties.getProperty(PropertyNames.UserID)).
        // Note: If Auth type is pem, then the UserPassword value would have path to the password file formatted as PEM
        option(authTypeVal,conProperties.getProperty(PropertyNames.AuthPasswordFile)).
        option("delimiter", ",").
        option("fileType", fileExtension).
        option("encoding", "UTF-8").

        // option("multiLine", "true").
        save(filePath)
    } catch {
      case e:Throwable=>  {
        bRet = false
        e.printStackTrace()
        throw new DataWriterException(e.getMessage, e.getCause)
      }
    }
    finally {
    }
    bRet
  }

  @throws(classOf[Exception])
  def sftpReadCsvFile(sparkSession: SparkSession, conProperties: Properties, filePath: String, authType: Int): DataFrame = {
    var dfCsv: org.apache.spark.sql.DataFrame = null
    log.info("AppProperties.CsvQuote => "+AppProperties.CsvQuote)
    log.info("AppProperties.CsvEscape => "+AppProperties.CsvEscape)
    try {
      val authTypeVal: String = getAuthTypeAsString(authType)
      dfCsv = sparkSession.read.
        format("com.springml.spark.sftp").
        option("host", conProperties.getProperty(PropertyNames.EndPoint)).
        option("username", conProperties.getProperty(PropertyNames.UserID)).
        // Note: If Auth type is pem, then the UserPassword value would have path to the password file formatted as PEM
        option(authTypeVal,conProperties.getProperty(PropertyNames.AuthPasswordFile)).
        option("fileType", Constants.CsvExtension).
        //option("delimiter", ";").
        //option("quote", "\"").
        option("quote", AppProperties.CsvQuote).
        option("escape", AppProperties.CsvEscape).
        option("multiLine", AppProperties.CsvMultiLine).
        option("inferSchema", AppProperties.CsvInferSchema).
        load(filePath)

    } finally {
    }
    dfCsv
  }


}

