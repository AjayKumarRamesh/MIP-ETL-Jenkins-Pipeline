package java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * The UTLookupTable program reads through the URLs from urls.csv that was loaded to COS from UmcUrlsWithNullUtValuesToCsv program
 * Then searches CMDP for those urls.
 * Writes the output to a csv ut_lookup_table.csv then pushes it to COS bucket
 * DrupalJsonCsv program uses this csv to generate the final drupal recomendation csv drupalReco.csv.
 *
 * @author  Richard Damelio
 * @version 1.0
 * @since   2021-06-21
 */
public class UTLookupTable {

    // Declare Endpoints
    private static final String MAP_DEV_COS_ENDPOINT_URL = "https://map-dev-01.s3.ap.cloud-object-storage.appdomain.cloud/drupal-recomendation/";
    private static final String COS_BUCKET = "urls_missing_ut_values/";
    private static final String URLS_CSV_ENDPOINT = "urls.csv";
    private static final String UT_LOOKUP_TABLE_ENDPOINT = "ut_lookup_table.csv";
    private static final String DEFAULT_IAM_ENDPOINT = "https://iam.cloud.ibm.com/identity/token?grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=";

    private static Map<String, String> searchForMissingUT10Data(Set<String> uniqueSearchStrings) throws Exception {

        Map<String, String> searchStringMap = new HashMap<String, String>();

        //JDBC connection info
        String ibmDriver = "com.ibm.db2.jcc.DB2Driver";
        String CMDP_ENDPOINT = System.getenv("CMDP_PROD_ENDPOINT");
        String TRUST_STORE_PATH = "cert/javacerts.jks";
        String CMDP_JKS_PASSWORD = System.getenv("CMDP_PROD_JKS_PASSWORD");
        String DbServerUrl = CMDP_ENDPOINT + ":sslConnection=true;sslTrustStoreLocation=" + TRUST_STORE_PATH + ";sslTrustStorePassword=" + CMDP_JKS_PASSWORD + ";";

        Connection myCon;
        Statement selectStmt = null;
        ResultSet selectResults = null;
        String DbUser = System.getenv("CMDP_PROD_USER_ID");
        String DbPass = System.getenv("CMDP_PROD_PASSWORD");

        // create an instance of a driver object.
        try { Class.forName( ibmDriver ); }
        catch ( ClassNotFoundException exception) {
            exception.printStackTrace() ;
        }

        myCon = DriverManager.getConnection(DbServerUrl, DbUser, DbPass); // Declare and connect to data source (CMDP):

        System.out.println("Searching on " + uniqueSearchStrings.size() + " unique searchStrings");
        // Cycle through URLs and return their results
        Iterator uniqueSearchStringIterator = uniqueSearchStrings.iterator();

        while (uniqueSearchStringIterator.hasNext()) {
            String currentSearchString = (String) uniqueSearchStringIterator.next();
            selectStmt = myCon.createStatement(); // myCon is the active connection
            selectResults = selectStmt.executeQuery("Select distinct UT10_CODE, UT10_NAME, UT15_CODE, UT15_NAME, UT17_CODE, UT17_NAME, UT20_CODE, UT20_NAME, UT30_CODE, UT30_NAME from V2DIGT2.V_URL_UT_XREF where PAGE_URL like '%" + currentSearchString + "%' AND UT10_CODE <> 'G0000'");

            while(true) {
                if (!selectResults.next()) break;
                String ut10Code = selectResults.getString("UT10_CODE");
                String ut10Name = selectResults.getString("UT10_NAME");
                String ut15Code = selectResults.getString("UT15_CODE");
                String ut15Name = selectResults.getString("UT15_NAME");
                String ut17Code = selectResults.getString("UT17_CODE");
                String ut17Name = selectResults.getString("UT17_NAME");
                String ut20Code = selectResults.getString("UT20_CODE");
                String ut20Name = selectResults.getString("UT20_NAME");
                String ut30Code = selectResults.getString("UT30_CODE");
                String ut30Name = selectResults.getString("UT30_NAME");

                // clear null values
                if (ut10Code == null || ut10Code.equals("null")) ut10Code = "";
                if (ut10Name == null || ut10Name.equals("null")) ut10Name = "";
                if (ut15Code == null || ut15Code.equals("null")) ut15Code = "";
                if (ut15Name == null || ut15Name.equals("null")) ut15Name = "";
                if (ut17Code == null || ut17Code.equals("null")) ut17Code = "";
                if (ut17Name == null || ut17Name.equals("null")) ut17Name = "";
                if (ut20Code == null || ut20Code.equals("null")) ut20Code = "";
                if (ut20Name == null || ut20Name.equals("null")) ut20Name = "";
                if (ut30Code == null || ut30Code.equals("null")) ut30Code = "";
                if (ut30Name == null || ut30Name.equals("null")) ut30Name = "";

                String result = ut10Code + "," + ut10Name + "," + ut15Code + "," + ut15Name + "," + ut17Code +
                        "," + ut17Name + "," + ut20Code + "," + ut20Name + "," + ut30Code + "," + ut30Name;

                searchStringMap.put(currentSearchString, result);
            }
        }

        selectStmt.close() ; //Close the statement
        myCon.close(); //Close the connection

        return searchStringMap;
    }

    private static String getSearchString(String url) throws Exception {

        // Remove protocol, domain name, query string and language code from urls

        String searchString = "";

        try {
            int startOfSearchString = 0;
            int endOfSearchString = url.length();

            // Start search string at the right of ".com", if present
            if (url.contains(".com/")) {
                startOfSearchString = url.indexOf(".com/") + 4;
            }
            else {
                // If no ".com" present, look for protocol and start just to the right of "://", if present
                if (url.contains("://")) {
                    startOfSearchString = url.indexOf("://") + 3;
                }
            }

            if (url.contains("?")) {
                endOfSearchString = url.indexOf("?");
            }

            searchString = url.substring(startOfSearchString, endOfSearchString);

            // Check if language local is part of URL and if so remove it
            if (searchString.contains("us-en/")) {
                int startOfUsenSearchString = url.indexOf("us-en/") + 5;
                searchString = url.substring(startOfUsenSearchString, endOfSearchString);
            }
        }

        catch(Exception ex) {
            throw new Exception("There was an issue getting the search string for: " + url);
        }

        return searchString;
    }

    public static void main(String[] args) {

        List<String> urls = new ArrayList<String>();

        try {
            // connect to COS (Cloud Object Storage service)
            String accessToken = getToken();
            String urlsMissingUTValuesCSV = MAP_DEV_COS_ENDPOINT_URL + COS_BUCKET + URLS_CSV_ENDPOINT;
            URL urlMissingUTValues = new URL(urlsMissingUTValuesCSV);
            HttpURLConnection connection = (HttpURLConnection) urlMissingUTValues.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", accessToken);
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;

            // Read urls that need to be looked up from COS bucket
            System.out.println("Reading from COS bucket csv");
            while ((line = br.readLine()) != null) {
                urls.add(line);
            }
            br.close();

            if (urls.size() == 0) {
                throw new Exception("No URLs were found to lookup.");
            }

            String searchString = "";
            Map<String, String> urlMap = new HashMap<String, String>(); // Hashmap #1
            Map<String, String> searchStringMap = new HashMap<String, String>(); // Hashmap #2
            Set<String> uniqueSearchStrings = new HashSet<String>(); // The only values that we will search for in CMDP

            /* Note: Hashmap 1 will store a unique list the full urls (as hashmap keys) with their search strings (as hashmap values)
               Hashmap 2 will store a unique list of the search strings (as hashmap keys) with their UT values (as hashmap values)
               Then only the search strings in Hashmap 2 are searched on in CMDP.
               The search strings and UT values from Hashmap 2 are mapped back to the full urls in Hashmap 1.  The combined
               output is stored in a csv in a COS bucket that can be used a lookup table for full urls missing UT values.
            */

            System.out.println("Fetching results for " + urls.size() + " urls");

            for (int i = 0; i < urls.size(); i++) {
                String url = urls.get(i);
                if (url == "https://www.ibm.com")
                    continue;
                searchString = getSearchString(url);
                urlMap.put(url, searchString);
                uniqueSearchStrings.add(searchString);
            }

            System.out.println("Start searching for missing UT data");

            searchStringMap = searchForMissingUT10Data(uniqueSearchStrings);

            System.out.println("Combining hashmaps to create output");
            Iterator urlMapIterator = urlMap.entrySet().iterator();
            StringBuilder csvSB = new StringBuilder();
            csvSB.append("\"URL\",\"UT10_CODE\",\"UT10_NAME\",\"UT15_CODE\",\"UT15_NAME\",\"UT17_CODE\",\"UT17_NAME\",\"UT20_CODE\",\"UT20_NAME\",\"UT30_CODE\",\"UT30_NAME\"\n");

            while (urlMapIterator.hasNext()) {
                Map.Entry mapElement = (Map.Entry)urlMapIterator.next();
                String currentSearchString = (String) mapElement.getValue();
                String utValues = searchStringMap.get(currentSearchString);
                //System.out.println("url: " + mapElement.getKey() + "searchString: " + mapElement.getValue() + "UT values: " + utValues);
                if (utValues == null) utValues = "";
                csvSB.append(mapElement.getKey() + "," + utValues + "\n");
            }

            System.out.println("Writing output csv in COS bucket");
            String lookupTableCsvEndpointURL = MAP_DEV_COS_ENDPOINT_URL + COS_BUCKET + UT_LOOKUP_TABLE_ENDPOINT;
            accessToken = getToken();

            int responseCodeLiveBucket = uploadFileinIBM_COS(lookupTableCsvEndpointURL,accessToken,csvSB);
            if (responseCodeLiveBucket / 100 == 2) {
                System.out.println("Completed");
            }
            else {
                throw new Exception("FAILED: Writing output to COS bucket");
            }
        }

        catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static int uploadFileinIBM_COS(String s3BucketUrl, String accessToken, StringBuilder csvSB) throws IOException {

        URL url = new URL (s3BucketUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", accessToken);
        OutputStream os = conn.getOutputStream();
        os.write(csvSB.toString().getBytes());
        os.flush();
        os.close();
        int responseCode = conn.getResponseCode();
        conn.disconnect();
        return responseCode;
    }

    private static String getToken() throws Exception {

        String accessToken = "";

        try {
            //Get API Key of the Service ID (Service Role) from Env Variables
            String accessKey = System.getenv("OW_IAM_NAMESPACE_API_KEY");
            String iamUrl = DEFAULT_IAM_ENDPOINT + accessKey;

            StringBuilder accessTempToken = new StringBuilder();
            String keyToken = "";
            URL urlGetToken = new URL(iamUrl);
            HttpURLConnection connGetToken = (HttpURLConnection) urlGetToken.openConnection();
            connGetToken.setRequestMethod("POST");
            connGetToken.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connGetToken.setRequestProperty("Accept","application/json");
            BufferedReader buffReader = new BufferedReader(new InputStreamReader(connGetToken.getInputStream()));
            String line;
            while ((line = buffReader.readLine()) != null) {
                accessTempToken.append(line);
            }
            buffReader.close();
            connGetToken.disconnect();
            JsonObject token = new JsonParser().parse(accessTempToken.toString()).getAsJsonObject();

            if(token.get("access_token") != null && !token.get("access_token").isJsonNull())
                keyToken = token.get("access_token").getAsString();

            accessToken = "Bearer " + keyToken;
        }
        catch(Exception ex){
            throw new Exception("Unable to retrieve access token");
        }

        return accessToken;
    }
}