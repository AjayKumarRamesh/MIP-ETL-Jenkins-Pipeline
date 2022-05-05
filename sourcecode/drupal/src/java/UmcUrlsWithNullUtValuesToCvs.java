package java;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * UmcUrlsWithNullUtValuesToCsv program gets the UT and NLU keywords from UMC
 * Then reads through the results from the drupal endpoint and generates a stringbuilder drupalJson
 * If the product_id != "" then we get the UT values from the UMC
 * Then we search for utlevel10code were it's == "" and appends the pageUrl to a stringbuilder umcUrls
 * stringbuilder umcUrls is then converted to string and uploaded to COS as urls.csv
 * UTLookupTable class uses urls.csv to search CMDP for the missing urls so we can get their UT values.
 *
 * @author  Richard Damelio
 * @version 1.0
 * @since   2021-06-21
 */
public class UmcUrlsWithNullUtValuesToCsv {

    // Declare Endpoints
    private static final String DEEFAULT_S3_ENDPOINT_URL = "https://s3.ap.cloud-object-storage.appdomain.cloud";
    private static final String DEFAULT_URLS_MISSING_UT_VALUES_BUCKT_NAME = "map-dev-01/drupal-recomendation/urls_missing_ut_values/";
    private static final String DEFAULT_DRUPAL_ENDPOINT = "https://www.ibm.com/internal/cms/clp/products?_format=json";
    private static final String DEFAULT_IAM_ENDPOINT = "https://iam.cloud.ibm.com/identity/token?grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=";
    private static final String DEFAULT_UMC_ENDPOINT = "https://www.ibm.com/marketplace/tools/sonar/api/v3/production/product-summary-list-v2?page=1&pageSize=1000";

    private static final String DATA_SOURCE = "Drupal";
    private static String[] contentType_cta_codes;
    private static String[] contentType_audience_codes;
    private static String UMC_Bearer_API_KEY = "";


    public static JsonObject main(JsonObject args) {

        String urls_missing_ut_values = "";
        String s3_endpoint_url = "";
        String drupal_endpoint_url = "";
        String responseCode = "";

        if(args.get("urls_missing_ut_values") != null && !args.get("urls_missing_ut_values").isJsonNull()){
            urls_missing_ut_values = args.get("urls_missing_ut_values").getAsString();
        }else {
            urls_missing_ut_values = DEFAULT_URLS_MISSING_UT_VALUES_BUCKT_NAME;
        }

        if(args.get("s3_endpoint_url") != null && !args.get("s3_endpoint_url").isJsonNull()){
            s3_endpoint_url = args.get("s3_endpoint_url").getAsString();
        }else {
            s3_endpoint_url = DEEFAULT_S3_ENDPOINT_URL;
        }

        if(args.get("drupal_endpoint_url") != null && !args.get("drupal_endpoint_url").isJsonNull()){
            drupal_endpoint_url = args.get("drupal_endpoint_url").getAsString();
        }else {
            drupal_endpoint_url = DEFAULT_DRUPAL_ENDPOINT;
        }


        String urls_missing_ut_values_bucket_csv_url = s3_endpoint_url + "/" +  urls_missing_ut_values  + "urls.csv";


        responseCode = getRecoProducts(drupal_endpoint_url,urls_missing_ut_values_bucket_csv_url);
        String jsonString = "{\"Result\": \"" + responseCode  + "\"}";
        JsonObject response = new JsonParser().parse(jsonString).getAsJsonObject();

        return response;
    }

    private static String getRecoProducts(String drupal_endpoint_url, String urls_missing_ut_values_bucket_csv_url) {

        StringBuilder umcUrls = new StringBuilder();
        StringBuilder drupalJson = new StringBuilder();
        Map<String, List<String>> utValueMap = new HashMap<String, List<String>>();
        String compositeNid = "";
        String responeCode = "";
        String nid= "";
        String nidString = "";
        String locale = "";
        String localeString = "";
        String product_id = "";
        String admin_id = "";
        String commonType = "";
        String field_content_category_id = "";
        String pre_headline = "";
        String headline = "";
        String description = "";
        String cta = "";
        String cta_cta_label = "";
        String cta_url = "";
        String cta_secondary = "";
        String cta_secondary_cta_label = "";
        String cta_secondary_url = "";
        String image_url = "";
        String purchasing = "";
        String purchasing_price_check = "";
        String purchasing_pricing_preview = "";
        String purchasing_pricing_details = "";
        String purchasing_pricing_uuid = "";
        String purchasing_sash = "";
        String purchasing_pricing_intro = "";
        String purchasing_promotional = "";
        String moderation_date = "";
        String moderation_state = "";
        String failedNid = "";
        String priority = "";
        String isActive = "";

        String utlevel10code = "";
        String utlevel15code = "";
        String utlevel17code = "";
        String utlevel20code = "";
        String utlevel30code = "";

        String utlevel10description = "";
        String utlevel15description = "";
        String utlevel17description = "";
        String utlevel20description = "";
        String utlevel30description = "";

        String prodCategory = "";
        String nluKeywords = "";
        String offeringType = "";


        JsonObject tempJsonObj = new JsonObject();

        try
        {
            //Retrieve token to create CSV files on IBM Cloud Object Storage.
            String accessToken = getToken();

            // Load content types from config.properties file
            loadContentTypes();

            //Get UT Map from UMC
            utValueMap = getUTfromUMC();

            //Call Drupal end point to retrieve the JSON response.
            URL urlDrupalJson = new URL(drupal_endpoint_url);
            HttpURLConnection connDrupalJsonRead = (HttpURLConnection) urlDrupalJson.openConnection();
            connDrupalJsonRead.setRequestMethod("GET");

            // Read the json into StringBuilder drupalJson
            BufferedReader rd = new BufferedReader(new InputStreamReader(connDrupalJsonRead.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                drupalJson.append(line);
            }

            // Close the Http connection
            rd.close();
            connDrupalJsonRead.disconnect();

            //Convert Drupal Json from StringBuilder to JSON Array Object drupalResponse for parsing
            JsonArray drupalResponse = new JsonParser().parse(drupalJson.toString()).getAsJsonArray();


            // Initialize all the keys that represent the keys in the drupal json array(drupalResponse)
            for(int i = 0; i < drupalResponse.size(); i++)
            {
                nid= "";
                nidString = "";
                locale = "";
                product_id = "";
                localeString = "";
                admin_id = "";
                commonType = "";
                field_content_category_id = "";
                pre_headline = "";
                headline = "";
                description = "";
                cta = "";
                cta_cta_label = "";


                cta_url = "";
                cta_secondary = "";
                cta_secondary_cta_label = "";
                cta_secondary_url = "";
                image_url = "";
                purchasing = "";
                purchasing_price_check = "";
                purchasing_pricing_preview = "";
                purchasing_pricing_details = "";
                purchasing_pricing_uuid = "";
                purchasing_sash = "";
                purchasing_pricing_intro = "";
                purchasing_promotional = "";
                moderation_date = "";
                moderation_state = "";
                compositeNid = "";
                priority = "";
                isActive = "";



                utlevel10code = "";
                utlevel15code = "";
                utlevel17code = "";
                utlevel20code = "";
                utlevel30code = "";

                utlevel10description = "";
                utlevel15description = "";
                utlevel17description = "";
                utlevel20description = "";
                utlevel30description = "";
                prodCategory = "";
                nluKeywords = "";
                offeringType = "";


                // Read through the json array(drupalResponse) and assign the values for each json/Node.
                try
                {
                    // Grab the next json in the json array (drupalResponse)
                    JsonObject childNode = drupalResponse.get(i).getAsJsonObject();

                    // Assign key values from json
                    if(childNode.get("locale")!= null && !childNode.get("locale").isJsonNull())
                    {
                        locale = childNode.get("locale").getAsString();
                        if(!( locale.equalsIgnoreCase("en-us") ) ){
                            continue;
                        }
                        else{
                            locale = childNode.get("locale").toString();
                            localeString = childNode.get("locale").getAsString();  // getAsString to create a Composite key = nid + locale
                        }
                    }

                    if(childNode.get("nid") != null && !childNode.get("nid").isJsonNull()){
                        nid = childNode.get("nid").toString();
                        nidString = childNode.get("nid").getAsString();  // getAsString to create a Composite key = nid + locale
                    }

                    compositeNid = "\"".concat(nidString + "-" + localeString) + "\"";

                    if(childNode.get("product_id") !=null &&  !childNode.get("product_id").isJsonNull() )
                        product_id = childNode.get("product_id").toString();

                    if(childNode.get("admin_id") != null && !childNode.get("admin_id").isJsonNull())
                        admin_id = childNode.get("admin_id").toString();

                    if(admin_id != ""){
                        commonType = getContentType(admin_id);  // Extract Common Type value from admin_id
                    }

                    if(childNode.get("field_content_category_id") !=null &&  !childNode.get("field_content_category_id").isJsonNull() )
                        field_content_category_id = childNode.get("field_content_category_id").toString();

                    if(childNode.get("pre_headline") != null && !childNode.get("pre_headline").isJsonNull())
                        pre_headline = childNode.get("pre_headline").toString();

                    if(childNode.get("headline") != null && !childNode.get("headline").isJsonNull())
                        headline = childNode.get("headline").toString();

                    if(childNode.get("description") != null && !childNode.get("description").isJsonNull())
                        description = childNode.get("description").toString();

                    if(childNode.get("cta") != null)
                    {
                        tempJsonObj = (JsonObject) childNode.get("cta");

                        if(tempJsonObj.get("cta_label") != null && !tempJsonObj.get("cta_label").isJsonNull())
                            cta_cta_label = tempJsonObj.get("cta_label").toString();

                        if(tempJsonObj.get("url") != null && !tempJsonObj.get("url").isJsonNull())
                        {
                            cta_url = tempJsonObj.get("url").toString();
                            
                            if(cta_url.contains("/en/")) {
                                cta_url = cta_url.replace("/en/", "/");
                            }
                            else if(cta_url.contains("/en-us/"))
                                cta_url = cta_url.replace("/en-us/", "/");                      
                          
                        }
                    }

                    if(childNode.get("image_url") != null && !childNode.get("image_url").isJsonNull())
                        image_url = childNode.get("image_url").toString();

                    if(childNode.get("purchasing") != null)
                    {
                        tempJsonObj = (JsonObject) childNode.get("purchasing");

                        if(tempJsonObj.get("price_check") != null && !tempJsonObj.get("price_check").isJsonNull())
                            purchasing_price_check = tempJsonObj.get("price_check").toString();

                        if(tempJsonObj.get("pricing_preview") != null && !tempJsonObj.get("pricing_preview").isJsonNull())
                            purchasing_pricing_preview = tempJsonObj.get("pricing_preview").toString();

                        if(tempJsonObj.get("pricing_details") != null && !tempJsonObj.get("pricing_details").isJsonNull())
                            purchasing_pricing_details = tempJsonObj.get("pricing_details").toString();

                        if(tempJsonObj.get("pricing_uuid") != null && !tempJsonObj.get("pricing_uuid").isJsonNull())
                            purchasing_pricing_uuid = tempJsonObj.get("pricing_uuid").toString();

                        if(tempJsonObj.get("sash") != null && !tempJsonObj.get("sash").isJsonNull())
                            purchasing_sash = tempJsonObj.get("sash").toString();

                        if(tempJsonObj.get("pricing_intro") != null && !tempJsonObj.get("pricing_intro").isJsonNull())
                            purchasing_pricing_intro = tempJsonObj.get("pricing_intro").toString();

                        if(tempJsonObj.get("promotional") != null && !tempJsonObj.get("promotional").isJsonNull())
                            purchasing_promotional = tempJsonObj.get("promotional").toString();

                    }

                    if(childNode.get("moderation_date") != null && !childNode.get("moderation_date").isJsonNull())
                        moderation_date = childNode.get("moderation_date").toString();

                    if(childNode.get("moderation_state") != null && !childNode.get("moderation_state").isJsonNull())
                        moderation_state = childNode.get("moderation_state").toString();


                    if(product_id != "") {

                        // Get the UT values from the UMC hashmap for each product_id
                        List<String> utValList = utValueMap.get(product_id);

                        if(utValList != null) {

                            utlevel10code = utValList.get(0);
                            utlevel10description = utValList.get(1);
                            utlevel15code = utValList.get(2);
                            utlevel15description = utValList.get(3);
                            utlevel17code = utValList.get(4);
                            utlevel17description = utValList.get(5);
                            utlevel20code = utValList.get(6);
                            utlevel20description = utValList.get(7);
                            utlevel30code = utValList.get(8);
                            utlevel30description = utValList.get(9);
                            prodCategory = utValList.get(10);			// <<===== Category - UT Values ****** //
                            nluKeywords = utValList.get(11);			// <<===== NLU Keywords ****** //
                            offeringType = utValList.get(12);          // <<===== Offering Type - UMC **********//
                        }
                    }


                    // If UT values from UMC are null then check CMDP
                    if(utlevel10code == "")
                    {
                        // TODO; call cmdp using cta_url

                        // Remove https:// and everything after the ? from the URL
                        String pageUrlOrig = cta_url;
                        //int positionOfQuestionMark = pageUrlOrig.indexOf("?");
                        //String pageString = pageUrlOrig.substring(9, positionOfQuestionMark); // 9 = length of "https://"

                        /*
                        // Remove www.ibm.com from URL
                        int startOfComSearchString = pageUrlOrig.indexOf(".com/") + 4;
                        String searchString = pageUrlOrig.substring(startOfComSearchString, positionOfQuestionMark);

                        // Check if language local is part of URL and if so remove it
                        if (searchString.contains("us-en/")) {
                            int startOfUsenSearchString = pageUrlOrig.indexOf("us-en/") + 5;
                            searchString = pageUrlOrig.substring(startOfUsenSearchString, positionOfQuestionMark);
                        }

                        //System.out.println("Parsed pageurl: " + searchString);

                         */

                        umcUrls.append(pageUrlOrig  + "\n");

                        //umcUrls
                        //Urls.add(pageUrlOrig);

                    }


                }
                catch(Exception ex)
                {
                    //Move to next record. Do not stop the entire process. Add failed id to the list.
                    failedNid = failedNid + nid + ",";
                }
            }

            int responseCodeMissingUt = uploadFileinIBM_COS(urls_missing_ut_values_bucket_csv_url,accessToken,umcUrls);

            responeCode  = "Missing UT Bucket : " +Integer.toString(responseCodeMissingUt);
            failedNid = failedNid.replace("\"","");
            responeCode = responeCode + " & " + "Failed Nid : " + failedNid;
        }
        catch(Exception ex) {
            //Catch exception and return error message to the calling function.
            responeCode = ex.getLocalizedMessage();
        }
        return responeCode;
    }

    //This section is defined only for running this on local machine. IBM Cloud Function call this method -> public static JsonObject main(JsonObject args)
    //For running on local, use S3 Public End-point. When using in IBM Function, us S3 Private End-point.

    public static void main(String[] args) {

        JsonObject response = new JsonObject();
        response.addProperty("urls_missing_ut_values", "map-dev-01/drupal-recomendation/urls_missing_ut_values/");
        response.addProperty("s3_endpoint_url", "https://s3.ap.cloud-object-storage.appdomain.cloud");
        response.addProperty("drupal_endpoint_url", "https://www.ibm.com/internal/cms/clp/products?_format=json");

        System.out.println("Main response: " + main(response));
        System.out.println("Action Completed : " + response);

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

// Parse Admin Id to generate Content Type

    private static void loadContentTypes() throws Exception  {

        Properties properties = new Properties();
        try
        {
            InputStream input = DrupalJsonToCsv.class.getClassLoader().getResourceAsStream("config.properties");
            properties.load(input);
            String contentTypeList = properties.getProperty("contentType_CTA_Types");
            String audienceCodes = properties.getProperty("contentType_Audience_Codes");
            contentType_audience_codes = audienceCodes.split("##");
            contentType_cta_codes = contentTypeList.split("##");
            UMC_Bearer_API_KEY = System.getenv("UMC_Bearer_API_KEY");

        }catch(Exception e) {
            throw new Exception("Unable to load property file for Content Typess");
        }
    }

    private static String getContentType(String adminId_inputVal) throws Exception {

        Optional<String> ctaType = Arrays.stream(contentType_cta_codes).filter(adminId_inputVal::contains).findAny();
        Optional<String> audienceCode = Arrays.stream(contentType_audience_codes).filter(adminId_inputVal::contains).findAny();

        if(ctaType.isPresent() && audienceCode.isPresent())
        {
            String contentTypeCTACode = ctaType.get();
            String contentTypeAudienceCode = audienceCode.get();
            String retString = "";

            contentTypeCTACode = contentTypeCTACode.substring(1, contentTypeCTACode.length() - 1);  // Remove first and last underscore.
            contentTypeAudienceCode = contentTypeAudienceCode.substring(1, contentTypeAudienceCode.length() - 1); // Remove first and last underscore.

            retString = "\"merchandising_tile_" + contentTypeAudienceCode + "_" +contentTypeCTACode +"\"";

            return retString;
        }

        return adminId_inputVal;
    }


// Get UT and NLU keywords from UMC

    private static Map<String, List<String>> getUTfromUMC() throws Exception {

        String productKey = "";

        String utlevel10code = "";
        String utlevel15code = "";
        String utlevel17code = "";
        String utlevel20code = "";
        String utlevel30code = "";

        String utlevel10codeString = "";
        String utlevel15codeString = "";
        String utlevel17codeString = "";
        String utlevel20codeString = "";
        String utlevel30codeString = "";

        String utlevel10description = "";
        String utlevel15description = "";
        String utlevel17description = "";
        String utlevel20description = "";
        String utlevel30description = "";

        String utlevel10descriptionString = "";
        String utlevel15descriptionString = "";
        String utlevel17descriptionString = "";
        String utlevel20descriptionString = "";
        String utlevel30descriptionString = "";

        String umcOfferingType = "";

        String nluKeyword01 = "";
        String nluKeyword02 = "";
        String nluKeyword03 = "";
        String nluKeyword04 = "";
        String nluKeyword05 = "";

        StringBuilder nluKeywords = new StringBuilder();
        StringBuilder drupalJson = new StringBuilder();
        List<String> utValues = new ArrayList<String>();
        Map<String, List<String>> utValMap = new HashMap<String, List<String>>();

        try
        {
            // UMC API Token - Get token from Property file
            URL urlDrupalJson = new URL(DEFAULT_UMC_ENDPOINT);
            HttpURLConnection connDrupalJsonRead = (HttpURLConnection) urlDrupalJson.openConnection();
            connDrupalJsonRead.setRequestMethod("GET");
            connDrupalJsonRead.setRequestProperty("Authorization", "Bearer "+ UMC_Bearer_API_KEY);
            BufferedReader rd = new BufferedReader(new InputStreamReader(connDrupalJsonRead.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                drupalJson.append(line);
            }
            rd.close();
            connDrupalJsonRead.disconnect();

            JsonObject drupalResponse = new JsonParser().parse(drupalJson.toString()).getAsJsonObject();
            JsonArray dataJsonArrray =    (JsonArray) drupalResponse.get("data");

            for(int i = 0; i < dataJsonArrray.size(); i++)
            {

                utlevel10code = "";
                utlevel15code = "";
                utlevel17code = "";
                utlevel20code = "";
                utlevel30code = "";

                utlevel10codeString = "";
                utlevel15codeString = "";
                utlevel17codeString = "";
                utlevel20codeString = "";
                utlevel30codeString = "";

                utlevel10description = "";
                utlevel15description = "";
                utlevel17description = "";
                utlevel20description = "";
                utlevel30description = "";

                utlevel10descriptionString = "";
                utlevel15descriptionString = "";
                utlevel17descriptionString = "";
                utlevel20descriptionString = "";
                utlevel30descriptionString = "";

                umcOfferingType = "";

                nluKeyword01 = "";
                nluKeyword02 = "";
                nluKeyword03 = "";
                nluKeyword04 = "";
                nluKeyword05 = "";

                nluKeywords = new StringBuilder();
                utValues = new ArrayList<String>();

                JsonObject umcJsonObj = dataJsonArrray.get(i).getAsJsonObject();
                productKey = umcJsonObj.get("productKey").toString();

                if( umcJsonObj.get("metadata")!= null && !umcJsonObj.get("metadata").isJsonNull())
                {
                    JsonObject metadatObj =   ((JsonObject) umcJsonObj.get("metadata"));
                    if( metadatObj.get("ibm-taxonomy")!= null && !metadatObj.get("ibm-taxonomy").isJsonNull())
                    {
                        JsonObject ibmTaxononyObj = (JsonObject)metadatObj.get("ibm-taxonomy");

                        if( ibmTaxononyObj.get("utlevel10")!= null && !ibmTaxononyObj.get("utlevel10").isJsonNull()){
                            utlevel10code = ibmTaxononyObj.get("utlevel10").toString();
                            utlevel10codeString = ibmTaxononyObj.get("utlevel10").getAsString();
                        }


                        if( ibmTaxononyObj.get("utlevel10description")!= null && !ibmTaxononyObj.get("utlevel10description").isJsonNull()) {
                            utlevel10description = ibmTaxononyObj.get("utlevel10description").toString();
                            utlevel10descriptionString = ibmTaxononyObj.get("utlevel10description").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel15")!= null && !ibmTaxononyObj.get("utlevel15").isJsonNull()){
                            utlevel15code = ibmTaxononyObj.get("utlevel15").toString();
                            utlevel15codeString = ibmTaxononyObj.get("utlevel15").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel15description")!= null && !ibmTaxononyObj.get("utlevel15description").isJsonNull()){
                            utlevel15description = ibmTaxononyObj.get("utlevel15description").toString();
                            utlevel15descriptionString = ibmTaxononyObj.get("utlevel15description").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel17")!= null && !ibmTaxononyObj.get("utlevel17").isJsonNull()){
                            utlevel17code = ibmTaxononyObj.get("utlevel17").toString();
                            utlevel17codeString = ibmTaxononyObj.get("utlevel17").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel17description")!= null && !ibmTaxononyObj.get("utlevel17description").isJsonNull()){
                            utlevel17description = ibmTaxononyObj.get("utlevel17description").toString();
                            utlevel17descriptionString = ibmTaxononyObj.get("utlevel17description").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel20")!= null && !ibmTaxononyObj.get("utlevel20").isJsonNull()){
                            utlevel20code = ibmTaxononyObj.get("utlevel20").toString();
                            utlevel20codeString = ibmTaxononyObj.get("utlevel20").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel20description")!= null && !ibmTaxononyObj.get("utlevel20description").isJsonNull()){
                            utlevel20description = ibmTaxononyObj.get("utlevel20description").toString();
                            utlevel20descriptionString = ibmTaxononyObj.get("utlevel20description").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel30")!= null && !ibmTaxononyObj.get("utlevel30").isJsonNull()){
                            utlevel30code = ibmTaxononyObj.get("utlevel30").toString();
                            utlevel30codeString = ibmTaxononyObj.get("utlevel30").getAsString();
                        }

                        if( ibmTaxononyObj.get("utlevel30description")!= null && !ibmTaxononyObj.get("utlevel30description").isJsonNull()){
                            utlevel30description = ibmTaxononyObj.get("utlevel30description").toString();
                            utlevel30descriptionString = ibmTaxononyObj.get("utlevel30description").getAsString();
                        }
                    }

                    if( metadatObj.get("offering-type")!= null && !metadatObj.get("offering-type").isJsonNull())
                    {
                        umcOfferingType = metadatObj.get("offering-type").toString();
                    }

                    if( metadatObj.get("taxonomies")!= null && !metadatObj.get("taxonomies").isJsonNull())
                    {
                        JsonArray nluKeywordsArray = (JsonArray)metadatObj.get("taxonomies");
                        String[] tempNluKeyword = {};
                        ArrayList<String> nluKeywordsArrayList = new ArrayList<String>();

                        for(int j=0; j<nluKeywordsArray.size(); j++)
                        {
                            tempNluKeyword = nluKeywordsArray.get(j).getAsString().split(":");

                            if(tempNluKeyword.length > 1)
                                nluKeywordsArrayList.add(tempNluKeyword[1]);
                            else
                                nluKeywordsArrayList.add(tempNluKeyword[0]);
                        }

                        nluKeywords.append("\"[");

                        int nluListSize = nluKeywordsArrayList.size();

                        for(int k=0; k<nluListSize; k++)
                        {
                            nluKeywords.append("\"\"" + nluKeywordsArrayList.get(k) + "\"\"");
                            if(k != nluListSize-1)
                            {
                                nluKeywords.append(",");
                            }
                        }

                        nluKeywords.append("]\"");

                    }
                }

                //Populate the Category Field with UT Values

                StringBuilder categorySB = new StringBuilder();
                categorySB.append("\"");

                if(utlevel10codeString != "") {
                    categorySB.append("\"\"" + utlevel10codeString + "\"\"");

                    if(utlevel15codeString != "") {
                        categorySB.append(",");
                        categorySB.append("\"\"" + utlevel10codeString + ":" + utlevel15codeString + "\"\"");

                        if(utlevel17codeString != "") {
                            categorySB.append(",");
                            categorySB.append("\"\"" + utlevel10codeString + ":" + utlevel15codeString + ":" + utlevel17codeString + "\"\"");

                            if(utlevel20codeString != "") {
                                categorySB.append(",");
                                categorySB.append("\"\"" + utlevel10codeString + ":" + utlevel15codeString + ":" + utlevel17codeString + ":" + utlevel20codeString + "\"\"");

                                if(utlevel30codeString != "") {
                                    categorySB.append(",");
                                    categorySB.append("\"\"" + utlevel10codeString + ":" + utlevel15codeString + ":" + utlevel17codeString + ":" + utlevel20codeString + ":" + utlevel30codeString + "\"\"");
                                }
                            }
                        }
                    }
                }

                if(utlevel10descriptionString != "") {
                    categorySB.append(",");
                    categorySB.append("\"\"" + utlevel10descriptionString + "\"\"");

                    if(utlevel15descriptionString != "") {
                        categorySB.append(",");
                        categorySB.append("\"\"" + utlevel10descriptionString + ":" + utlevel15descriptionString + "\"\"");

                        if(utlevel17descriptionString != "") {
                            categorySB.append(",");
                            categorySB.append("\"\"" + utlevel10descriptionString + ":" + utlevel15descriptionString + ":" + utlevel17descriptionString + "\"\"");

                            if(utlevel20descriptionString != "") {
                                categorySB.append(",");
                                categorySB.append("\"\"" + utlevel10descriptionString + ":" + utlevel15descriptionString + ":" + utlevel17descriptionString + ":" + utlevel20descriptionString + "\"\"");

                                if(utlevel30descriptionString != "") {
                                    categorySB.append(",");
                                    categorySB.append("\"\"" + utlevel10descriptionString + ":" + utlevel15descriptionString + ":" + utlevel17descriptionString + ":" + utlevel20descriptionString + ":" + utlevel30descriptionString + "\"\"");
                                }
                            }
                        }
                    }
                }

                categorySB.append("\"");

                utValues.add(utlevel10code);				// Position 00
                utValues.add(utlevel10description);			// Position 01
                utValues.add(utlevel15code);				// Position 02
                utValues.add(utlevel15description);			// Position 03
                utValues.add(utlevel17code);				// Position 04
                utValues.add(utlevel17description);			// Position 05
                utValues.add(utlevel20code);				// Position 06
                utValues.add(utlevel20description);			// Position 07
                utValues.add(utlevel30code);				// Position 08
                utValues.add(utlevel30description);		    // Position 09
                utValues.add(categorySB.toString());		// Position 10 - UT Keywords String
                utValues.add(nluKeywords.toString());       // Position 11 - NLU Keywords String
                utValues.add(umcOfferingType);				// Position 12 - Offering Type

                utValMap.put(productKey, utValues);

            }
        }catch(Exception e) {
            throw new Exception("Unable to retrieve the UT values from UMC");
        }
        return utValMap;

    }
}