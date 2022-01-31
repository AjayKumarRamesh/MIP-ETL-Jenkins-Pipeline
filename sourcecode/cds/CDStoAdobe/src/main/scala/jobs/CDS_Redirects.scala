package jobs

import java.io.{BufferedReader, FileReader, FileWriter, IOException}
import java.net.{HttpURLConnection, URL}

object CDS_Redirects {

  val basePath = "/Users/tmfiggins/IdeaProjects/CDStoAdobe/"

  def main(args: Array[String]): Unit = {
    var reader: BufferedReader = null
    try {
      reader = new BufferedReader(new FileReader(
        basePath + "src/main/test/urls.txt"));
      var line = reader.readLine();
      val myWriter: FileWriter = new FileWriter(basePath + "src/main/test/output.txt", false);
      while (line != null)
      { val url: URL = new URL("https://" + line + "/");
        val huc: HttpURLConnection  = url.openConnection().asInstanceOf[HttpURLConnection];
        huc.setInstanceFollowRedirects(false);
        val responseCode: Int = huc.getResponseCode();
        System.out.println(url + " " + responseCode);
        myWriter.write(url + " " + responseCode + "\n"); // read next line
        line = reader.readLine(); }
        reader.close();
        myWriter.close();
        } catch {
          case e: Throwable =>
          e.printStackTrace()
    }
  }
}

