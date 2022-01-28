
package SampleJobs.adhocdata


import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}

import com.ibm.mkt.etlframework.{AppProperties, Logging, PropertyNames}
import com.jcraft.jsch._
import org.apache.spark.sql.DataFrame
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable.ListBuffer

/**
 * This java program shows you how to make SFTP/SSH connection with public key
 * using JSch java API
 *
 * @author https://kodehelp.com
 */
object SFTPinJava extends Logging{
  /**
   * @param args
   */
  def main(args: Array[String]): Unit = {
    /*
            * Below we have declared and defined the SFTP HOST, PORT, USER and Local
            * private key from where you will make connection
            */
    val SFTPHOST = "sftp.datatransferhub.com"
    val SFTPPORT = 22
    val SFTPUSER = "354vqy865"
    // this file can be id_rsa or id_dsa based on which algorithm is used to create the key
    val privateKey = "./cert/marketo_sftp_pem.pem"
    //val privateKey = "~/Downloads/marketo_sftp_pem"
    val SFTPWORKINGDIR = "/mkto-datatransferhub-aws-transfer-us-east-2-prod/354vqy865/"
    val jSch:JSch = new JSch
    var session: Session = null
    var channel:Channel = null
    var channelSftp:ChannelSftp = null
    try {
      jSch.addIdentity(privateKey)
      System.out.println("Private Key Added.")
      session = jSch.getSession(SFTPUSER, SFTPHOST, SFTPPORT)
      System.out.println("session created.")
      val config = new Properties
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config)
      session.connect()
      channel = session.openChannel("sftp")
      channel.connect()
      System.out.println("shell channel connected....")
      channelSftp = channel.asInstanceOf[ChannelSftp]
      channelSftp.cd(SFTPWORKINGDIR)
      System.out.println("Changed the directory...")
      System.out.println("Listing files/folders...")
      val fileList = channelSftp.ls("./person_response").toArray.map(_.toString).toList
      val dateFormatToUse = new SimpleDateFormat("yyyy-MM-dd'T'HH_mm_ss.SSSSSS'Z'")
      val timeZone = "UTC"
      dateFormatToUse.setTimeZone(TimeZone.getTimeZone(timeZone))
      val compareDateString = "2021-05-20T20_57_09.841504Z"
      val compareDateParsed = dateFormatToUse.parse(compareDateString)
      val filePrefix = "person-response-"
      val fileExtension = ".csv"
      val filesToParse = getFilesToParse(fileList, compareDateParsed, dateFormatToUse, filePrefix, fileExtension)
      System.out.println("filesToParse => "+filesToParse)

    } catch {
      case e: JSchException =>
        e.printStackTrace()
      case e: SftpException =>
        e.printStackTrace()
    } finally {
      if (channelSftp != null) {
        channelSftp.disconnect()
        channelSftp.exit()
      }
      if (channel != null) channel.disconnect()
      if (session != null) session.disconnect()
    }
  }

  def getFilesToParse(fileList: List[String], runDate: Date, dateFormat: SimpleDateFormat,
                      filePrefix: String, fileExtension: String): List[String] = {
    val filesToParse: ListBuffer[String] = ListBuffer[String]()
    fileList.foreach(fileString => {

      val fileName = fileString.substring(fileString.indexOf(filePrefix))
      System.out.println("File => "+fileName)
      val fileTimestamp = fileName.substring(filePrefix.length).stripSuffix(fileExtension)

      System.out.println("fileTimestamp => "+fileTimestamp)
      var dateParsed = dateFormat.parse(fileTimestamp)
      System.out.println("File => "+fileName + ", Timestamp => "+fileTimestamp+", dateParsed => "+dateParsed)


      if (runDate.before(dateParsed)) {
        System.out.println(s"Date [$runDate] is before date [$dateParsed]")
        filesToParse.append(fileName)
      } else if (runDate.after(dateParsed)) {
        System.out.println(s"Date [$runDate] is after date [$dateParsed]")
      }

    })
    filesToParse.toList
  }
  @throws(classOf[Exception])
  def getFilesToParseV2(connProperties: Properties, sftpWorkingDir: String, responseFileFolder: String, responseFilePrefix: String, fileExtension: String, runDate: Date): DataFrame = {
    log.info("sftpWorkingDir => "+sftpWorkingDir)
    val dateFormatToUse = new SimpleDateFormat("yyyy-MM-dd'T'HH_mm_ss.SSSSSS'Z'")
    val timeZone = "UTC"
    val sftpHost = connProperties.getProperty(PropertyNames.EndPoint)
    val sftpUser = connProperties.getProperty(PropertyNames.UserID)
    val sftpPort = 22
    val privateKey = connProperties.getProperty(PropertyNames.AuthPasswordFile)
    log.info(privateKey)
    val filePrefix = responseFilePrefix  //"person-response-"
    log.info("responseFilePrefix => "+responseFilePrefix)
    log.info("fileExtension => "+fileExtension)
    val jSch:JSch = new JSch
    var session: Session = null
    var channel:Channel = null
    var channelSftp:ChannelSftp = null
    val filesToParse: ListBuffer[String] = ListBuffer[String]()
    var df: DataFrame= null
    try {
      jSch.addIdentity(privateKey)
      log.info("Private Key Added.")
      session = jSch.getSession(sftpUser, sftpHost, sftpPort)
      log.info("session created.")
      val config = new Properties
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config)
      session.connect()
      channel = session.openChannel("sftp")
      channel.connect()
      log.info("shell channel connected....")
      channelSftp = channel.asInstanceOf[ChannelSftp]
      channelSftp.cd(sftpWorkingDir)
      log.info("Changed the directory...")
      log.info("Listing files/folders...")
      // Response file folder like ./person_response
      log.info("responseFileFolder => "+responseFileFolder)
      val fileList = channelSftp.ls(responseFileFolder).toArray.map(_.toString).toList
      log.info("runDate => "+(new DateTime(runDate).withZone(DateTimeZone.forID(timeZone)).toString()))
      val fileSuffix: String = s".$fileExtension"
      fileList.foreach(fileString => {
        log.info("fileString => "+fileString)
        val fileName = fileString.substring(fileString.indexOf(filePrefix))
        log.info("File => "+fileName)
        val fileTimestamp = fileName.substring(filePrefix.length).stripSuffix(fileSuffix)
        log.info("fileTimestamp => "+fileTimestamp)
        val dateParsed = dateFormatToUse.parse(fileTimestamp)
        // TODO: Set responseFileTimestamp parsed from responseFileName.
        //  Avoid using global variable responseFileTimestamp if possible
        //responseFileTimestamp = dateParsed
        log.info("File => "+fileName + ", Timestamp => "+fileTimestamp+", dateParsed => "+dateParsed)
        if (runDate.before(dateParsed)) {
          log.info(s"Date [$runDate] is before date [$dateParsed]")
          val filePathToScan = s"""${responseFileFolder.stripSuffix("/")}/$fileName""" //responseFileFolder+"/"+fileName //
         // s"""${responseFileFolder.stripSuffix("/")}/$fileName"""
          log.info(filePathToScan)

          filesToParse.append(filePathToScan)
          // scan for errors
          df = SFTPUtil.sftpReadCsvFile(AppProperties.SparkSession, connProperties, filePathToScan, SFTPUtil.AuthTypePasswordFile)

        } else if (runDate.after(dateParsed)) {
          log.info(s"Date [$runDate] is after date [$dateParsed]")
        }
      })
    } catch {
      case e: JSchException =>
        e.printStackTrace()
        throw new Exception(s"${e.getMessage} - ${e.getCause}", e)
      case e: SftpException =>
        e.printStackTrace()
        throw new Exception(s"${e.getMessage} - ${e.getCause}", e)
    } finally {
      if (channelSftp != null) {
        channelSftp.disconnect()
        channelSftp.exit()
      }
      if (channel != null) channel.disconnect()
      if (session != null) session.disconnect()
    }
    df
  }


















}
