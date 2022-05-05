package java;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;

/**
 * DrupalJsonCsv program gets the UT and NLU keywords from UMC
 * Then reads through the results from the drupal endpoint and generates a stringbuilder drupalJson
 * If the product_id != "" then we get the UT values from the UMC
 * Get UT Values from ut_lookup_table.csv that was loaded to COS from UTLookupTable program
 * Get Priority and isActive Values from COS, Merch_Drupal_Nodes_with_isactive_value_priority.csv
 * Push drupalReco.csv to FTP server "w3-transfer.boulder.ibm.com"
 *
 * @author  Richard Damelio and Timothy Figgins
 * @version 1.1
 * @since   2021-06-21
 */
public class DrupalJsonToCsv {


    // Declare Endpoints
    private static final String DEFAULT_S3_ENDPOINT_URL = "https://s3.ap.cloud-object-storage.appdomain.cloud";
    private static final String DEFAULT_S3_LIVE_BUCKET_NAME = "map-dev-01/drupal-recomendation/live/";
    private static final String DEFAULT_S3_ARCHIVE_BUCKET_NAME = "map-dev-01/drupal-recomendation/archive/";
    private static final String DEFAULT_FTP_ENDPOINT_URL = "9.16.18.127";
    //private static final String DEFAULT_FTP_ENDPOINT_URL = "w3-transfer.boulder.ibm.com";
    private static final String DEFAULT_FTP_LIVE_BUCKET_NAME = "/www/prot/map_etl";
    private static final String DEFAULT_FTP_ARCHIVE_BUCKET_NAME = "/www/prot/map_etl/archive";
    private static final String DEFAULT_DRUPAL_ENDPOINT = "https://www.ibm.com/internal/cms/clp/products?_format=json";
    private static final String DEFAULT_IAM_ENDPOINT = "https://iam.cloud.ibm.com/identity/token?grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=";
    private static final String DEFAULT_UMC_ENDPOINT = "https://www.ibm.com/marketplace/tools/sonar/api/v3/production/product-summary-list-v2?page=1&pageSize=1000";
    //private static final String DR_EXTRACT_CSV_ENDPOINT = "https://map-dev-01.s3.ap.cloud-object-storage.appdomain.cloud/drupal-recomendation/dr_extract_csv/DR-Extract.csv";
    private static final String PRIORITY_CSV_ENDPOINT = "https://map-dev-01.s3.ap.cloud-object-storage.appdomain.cloud/drupal-recomendation/priority_csv/Merch_Drupal_Nodes_with_isactive_value_priority.csv";
    private static final String DEFAULT_URLS_MISSING_UT_VALUES_LOOKUP_BUCKET_NAME = "https://map-dev-01.s3.ap.cloud-object-storage.appdomain.cloud/drupal-recomendation/urls_missing_ut_values/ut_lookup_table.csv";

    private static final String DATA_SOURCE = "Drupal";
    private static String[] contentType_cta_codes;
    private static String[] contentType_audience_codes;
    private static String UMC_Bearer_API_KEY = "";
    private static String ENV = "";


    public static JsonObject main(JsonObject args) {

        String live_bucket_name = "";
        String archive_bucket_name = "";
        String s3_endpoint_url = "";
        String drupal_endpoint_url = "";
        String responseCode = "";

        if(args.get("live_bucket_name") != null && !args.get("live_bucket_name").isJsonNull()){
            live_bucket_name = args.get("live_bucket_name").getAsString();
        }else {
            live_bucket_name = DEFAULT_S3_LIVE_BUCKET_NAME;
        }

        if(args.get("archive_bucket_name") != null && !args.get("archive_bucket_name").isJsonNull()){
            archive_bucket_name = args.get("archive_bucket_name").getAsString();
        }else {
            archive_bucket_name = DEFAULT_S3_ARCHIVE_BUCKET_NAME;
        }

        if(args.get("s3_endpoint_url") != null && !args.get("s3_endpoint_url").isJsonNull()){
            s3_endpoint_url = args.get("s3_endpoint_url").getAsString();
        }else {
            s3_endpoint_url = DEFAULT_S3_ENDPOINT_URL;
        }

        if(args.get("drupal_endpoint_url") != null && !args.get("drupal_endpoint_url").isJsonNull()){
            drupal_endpoint_url = args.get("drupal_endpoint_url").getAsString();
        }else {
            drupal_endpoint_url = DEFAULT_DRUPAL_ENDPOINT;
        }


        String s3_live_bucket_csv_url = s3_endpoint_url + "/" +  live_bucket_name  + "drupalReco.csv";
        String s3_archive_bucket_csv_url = s3_endpoint_url + "/" + archive_bucket_name + "drupalReco-";

        responseCode = getRecoProducts(drupal_endpoint_url,s3_live_bucket_csv_url,s3_archive_bucket_csv_url);
        String jsonString = "{\"Result\": \"" + responseCode  + "\"}";
        System.out.println(jsonString);
        JsonObject response = new JsonParser().parse(jsonString).getAsJsonObject();

        return response;
    }

    private static String getRecoProducts(String drupal_endpoint_url, String s3_live_bucket_csv_url, String s3_archive_bucket_csv_url ) {

        StringBuilder drupalJson = new StringBuilder();
        StringBuilder csvSB = new StringBuilder();
        Map<String, List<String>> utValueMap = new HashMap<String, List<String>>();
        Map<String, List<String>> utValueMapFromDR = new HashMap<String, List<String>>();
        Map<String, List<String>> priorityMap = new HashMap<String, List<String>>();
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
        String industryList = "";
        String cta_url_path = "";
        String cta_url_domain = "";

        JsonObject tempJsonObj = new JsonObject();

        try
        {
            //Retrieve token to create CSV files on IBM Cloud Object Storage.
            String accessToken = getToken();

            // Load content types from config.properties file
            loadContentTypes();

            //Get UT Map from UMC
            utValueMap = getUTfromUMC();

            //Get UT Values from DR
            utValueMapFromDR = getDataFromDR();

            //Get Priority and isActive Values from CSV
            priorityMap = getPriorityFromCSV();

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


            /*    CSV content generation - START	*/

            //Mandatory entries in the Target Recommendation Feed
            csvSB.append("## RECSRecommendations Upload File" + "\n");
            csvSB.append("## RECS''## RECS'' indicates a Recommendations pre-process header. Please do not remove these lines." + "\n");
            csvSB.append("## RECS" + "\n");
            csvSB.append("## RECSUse this file to upload product display information to Recommendations. Each product has its own row. Each line must contain 19 values and if not all are filled a space should be left." + "\n");
            csvSB.append("## RECSThe last 100 columns (entity.custom1 - entity.custom100) are custom. The name 'customN' can be replaced with a custom name such as 'onSale' or 'brand'." + "\n");
            csvSB.append("## RECSIf the products already exist in Recommendations then changes uploaded here will override the data in Recommendations. Any new attributes entered here will be added to the product''s entry in Recommendations." + "\n");

            //CSV Header. First 19 columns are required by Target.
            csvSB.append("\"## RECSentity.id\",\"entity.name\",\"entity.categoryId\",\"entity.message\",\"entity.thumbnailUrl\",\"entity.value\",\"entity.pageUrl\",\"entity.inventory\",\"entity.margin\",\"entity.common-type\",\"entity.common-contentId\",\"entity.common-locale\",\"entity.product-promotional\",\"entity.merch-ctaSecondaryLabel\",\"entity.merch-ctaSecondaryUrl\",\"entity.merch-UTC\",\"entity.merch-headline\",\"entity.merch-description\",\"entity.merch-preHeadline\",\"entity.merch-ctaLabel\",\"entity.ut-level10code\",\"entity.ut-level10\",\"entity.ut-level15code\",\"entity.ut-level15\",\"entity.ut-level17code\",\"entity.ut-level17\",\"entity.ut-level20code\",\"entity.ut-level20\",\"entity.ut-level30code\",\"entity.ut-level30\",\"entity.common-source\",\"entity.common-topics\",\"entity.product-offeringType\",\"entity.merch-isactive\",\"entity.merch-priority\",\"entity.merch-industry\",\"entity.merch-ctaUrlDomain\",\"entity.merch-ctaUrlPath\"" + "\n");
            //	csvSB.append("\"## RECSentity.id\",\"entity.name\",\"entity.categoryId\",\"entity.message\",\"entity.thumbnailUrl\",\"entity.value\",\"entity.pageUrl\",\"entity.inventory\",\"entity.margin\",\"entity.common-type\",\"entity.common-contentId\",\"entity.common-locale\",\"entity.product-promotional\",\"entity.merch-ctaSecondaryLabel\",\"entity.merch-ctaSecondaryUrl\",\"entity.merch-UTC\",\"entity.merch-headline\",\"entity.merch-description\",\"entity.merch-preHeadline\",\"entity.merch-ctaLabel\",\"entity.ut-level10code\",\"entity.ut-level10\",\"entity.ut-level15code\",\"entity.ut-level15\",\"entity.ut-level17code\",\"entity.ut-level17\",\"entity.ut-level20code\",\"entity.ut-level20\",\"entity.ut-level30code\",\"entity.ut-level30\",\"entity.common-source\",\"entity.common-topics\",\"entity.product-offeringType\"" + "\n");

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
                String pageUrlOrig ="";
                industryList = "";
                cta_url_path = "";
                cta_url_domain = "";

                // Read through the json array(drupalResponse) and assign the values for each json/Node.
                try
                {
                    // Grab the next json in the json array (drupalResponse)
                    JsonObject childNode = drupalResponse.get(i).getAsJsonObject();

                    // Assign key values from json
                    if(childNode.get("locale")!= null && !childNode.get("locale").isJsonNull())
                    {
                        /*locale = childNode.get("locale").getAsString();
                        if(!( locale.equalsIgnoreCase("en-us") ) ){
                            continue;
                        }
                        else{ */

                        // Now accepting all locales than just "en-us"
                        locale = childNode.get("locale").toString();
                        localeString = childNode.get("locale").getAsString();  // getAsString to create a Composite key = nid + locale
                        //}

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

//                            if(cta_url.contains("/en/")) {
//                                cta_url = cta_url.replace("/en/", "/");
//                            }
                            if(cta_url.contains("/en-us/"))
                                cta_url = cta_url.replace("/en-us/", "/");
                        }
                        // This code adds two new fields to the CSV - "cta_url_domain" and "cta_url_path"

                        String tempCtaUrlString = tempJsonObj.get("url").getAsString();
                        int dotComIndex = tempCtaUrlString.indexOf(".com");

                        if(tempCtaUrlString.indexOf("marketplace") == -1)
                        {
                            if(tempCtaUrlString.indexOf("/" + localeString + "/") != -1)
                            {
                                String[] urlElements = tempCtaUrlString.split("/" + localeString + "/");
                                cta_url_domain = "\"" + urlElements[0] + "/\"";   //This code is intended to return ==>  "https://www.ibm.com/". (Trailing "/" included)
                                cta_url_path = "\"/" + urlElements[1] + "\"";	  //This code is intended to return ==>  "/path01/path021?query1=123". (Staring "/" included)
                            }
                            else if(!tempCtaUrlString.equalsIgnoreCase("https://www.ibm.com"))
                            {
                                cta_url_domain = "\"" + tempCtaUrlString.substring(0,dotComIndex + 5) + "\"";    //This code is intended to return ==>  "https://www.ibm.com/". ("/" included)
                                cta_url_path = "\"" + tempCtaUrlString.substring(dotComIndex + 4,tempCtaUrlString.length())  + "\"";	//This code is intended to return ==>  "/path01/path021?query1=123". (Staring "/" included)
                            }
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
                            industryList = utValList.get(13);          // <<===== Industry List - UMC **********//
                        }
                    }



                    if(utlevel10code == "")
                    {

                        String nidLocale = nidString + "-" + localeString;
                        pageUrlOrig = cta_url;
                        // Get record from ut_lookup_table.csv
                        List<String> utValFromDR = utValueMapFromDR.get(pageUrlOrig);
                        StringBuilder categorySB = new StringBuilder();

                        if(utValFromDR != null) {

                            int len = utValFromDR.size();
                            if(len >= 2 && utValFromDR.get(1) != null && !utValFromDR.get(1).equalsIgnoreCase(""))
                            {
                                categorySB.append("\"");
                                utlevel10code = "\"" + utValFromDR.get(1) + "\"" ;
                                categorySB.append("\"\"" + utValFromDR.get(1) + "\"\"");
                                //System.out.print(utValFromDR.get(1));
                                //System.out.print(categorySB.toString());
                                

                                if(len >= 4 && utValFromDR.get(3) != null && !utValFromDR.get(3).equalsIgnoreCase("") )
                                {
                                    categorySB.append(",");
                                    utlevel15code = "\"" + utValFromDR.get(3) + "\"" ;
                                    categorySB.append("\"\"" + utValFromDR.get(1) + ":" + utValFromDR.get(3) + "\"\"");

                                    if(len >= 6 && utValFromDR.get(5) != null && !utValFromDR.get(5).equalsIgnoreCase("") )
                                    {
                                        categorySB.append(",");
                                        utlevel17code = "\"" + utValFromDR.get(5) + "\"" ;
                                        categorySB.append("\"\"" + utValFromDR.get(1) + ":" + utValFromDR.get(3) + ":" + utValFromDR.get(5) + "\"\"");

                                        if(len >= 8 && utValFromDR.get(7) != null && !utValFromDR.get(7).equalsIgnoreCase(""))
                                        {
                                            categorySB.append(",");
                                            utlevel20code = "\"" + utValFromDR.get(7) + "\"" ;
                                            categorySB.append("\"\"" + utValFromDR.get(1) + ":" + utValFromDR.get(3) + ":" + utValFromDR.get(5) + ":" + utValFromDR.get(7) + "\"\"");

                                            if(len >= 10 && utValFromDR.get(9) != null && !utValFromDR.get(9).equalsIgnoreCase(""))
                                            {
                                                categorySB.append(",");
                                                utlevel30code = "\"" + utValFromDR.get(9) + "\"" ;
                                                categorySB.append("\"\"" + utValFromDR.get(1) + ":" + utValFromDR.get(3) + ":" + utValFromDR.get(5) + ":" + utValFromDR.get(7) + ":" + utValFromDR.get(9) + "\"\"");
                                            }
                                        }
                                    }
                                }

                                if(len >= 3 && utValFromDR.get(2) != null && !utValFromDR.get(2).equalsIgnoreCase(""))
                                {
                                    categorySB.append(",");
                                    utlevel10description = "\"" + utValFromDR.get(2) + "\"" ;
                                    categorySB.append("\"\"" + utValFromDR.get(2) + "\"\"");

                                    if(len >= 5 && utValFromDR.get(4) != null && !utValFromDR.get(4).equalsIgnoreCase(""))
                                    {
                                        categorySB.append(",");
                                        utlevel15description = "\"" + utValFromDR.get(4) + "\"" ;
                                        categorySB.append("\"\"" + utValFromDR.get(2) + ":" + utValFromDR.get(4) + "\"\"");

                                        if(len >= 7 && utValFromDR.get(6) != null && !utValFromDR.get(6).equalsIgnoreCase(""))
                                        {
                                            categorySB.append(",");
                                            utlevel17description = "\"" + utValFromDR.get(6) + "\"" ;
                                            categorySB.append("\"\"" + utValFromDR.get(2) + ":" + utValFromDR.get(4) + ":" + utValFromDR.get(6) + "\"\"");

                                            if(len >= 9 && utValFromDR.get(8) != null && !utValFromDR.get(8).equalsIgnoreCase(""))
                                            {
                                                categorySB.append(",");
                                                utlevel20description = "\"" + utValFromDR.get(8) + "\"" ;
                                                categorySB.append("\"\"" + utValFromDR.get(2) + ":" + utValFromDR.get(4) + ":" + utValFromDR.get(6) + ":" + utValFromDR.get(8) + "\"\"");

                                                if(len >= 11 && utValFromDR.get(10) != null && !utValFromDR.get(10).equalsIgnoreCase(""))
                                                {
                                                    categorySB.append(",");

                                                    if(!(utValFromDR.get(10).contains("\"")))
                                                        utlevel30description = "\"" + utValFromDR.get(10) + "\"" ;
                                                    else
                                                        utlevel30description = utValFromDR.get(10);

                                                    categorySB.append("\"\"" + utValFromDR.get(2) + ":" + utValFromDR.get(4) + ":" + utValFromDR.get(6) + ":" + utValFromDR.get(8) + ":" + utValFromDR.get(10).replace("\"", "") + "\"\"");
                                                }
                                            }
                                        }
                                    }
                                }

                                categorySB.append("\"");
                            }
                        }
                        prodCategory = categorySB.toString();
                    }



                    String nidLocale = nidString + "-" + localeString;
                    //List<String> priorityValList = priorityMap.get(nidLocale);
                    List<String> priorityValList = priorityMap.get(nidString);
                    if(priorityValList != null)
                    {
                        isActive = priorityValList.get(1);
                        priority = priorityValList.get(2);
                    }



                    //csvSB.append("\"## RECSentity.id\", \"entity.name\",\"entity.categoryId\",\"entity.message\",\"entity.thumbnailUrl\",   \"entity.value\"    ,\"entity.pageUrl\",\"entity.inventory\",\"entity.margin\",\"entity.common-type\"  ,\"entity.common-content-Id\",\"entity.common-locale\",\"entity.product-promotional\",\"entity.merch-cta-Secondary-Label\",\"entity.merch-cta-Secondary-Url\", \"entity.merch-UTC\"  ,\"entity.merch-headline\",\"entity.merch-description\",\"entity.merch-pre-Headline\",\"entity.merch-cta-Label\",\"entity.ut-level_10_code\",   \"entity.ut-level_10\"   ,\"entity.ut-level_15_code\",    \"entity.ut-level_15\"   ,\"entity.ut-level_17_code\",    \"entity.ut-level_17\"   ,\"entity.ut-level_20_code\",    \"entity.ut-level_20\"   ,\"entity.ut-level_30_code\",  \"entity.ut-level_30\"    ,\"entity.common-source\", \"entity.nluKeywords\",\"entity.offeringType\", \"entity.merch-isactive\",\"entity.merch-priority\"" +  "\n");

                    csvSB.append(      compositeNid + "," + headline + "," +  prodCategory +  "," + description + ","          +        "," + purchasing_sash + "," +  cta_url   + ","          +        ","       +      "," +    commonType    + "," +      product_id      + "," +      locale      + "," + purchasing_promotional + "," +   cta_secondary_cta_label    + "," +     cta_secondary_url      + "," + moderation_date + "," +     headline      + "," +     description      + "," +     pre_headline      + "," +      cta_cta_label  + "," +   utlevel10code    + "," + utlevel10description + ","  +   utlevel15code    + ","  + utlevel15description + ","  +  utlevel17code     + ","  + utlevel17description + ","  +  utlevel20code   +   ","  + utlevel20description + ","  +   utlevel30code    + "," + utlevel30description + "," +  DATA_SOURCE    +   "," +   nluKeywords  +  "," +   offeringType + ","   +   isActive      +  ","   +       priority   +  ","  +  industryList +  ","  +  cta_url_domain +  ","  +    cta_url_path     + "\n");
                    // 	csvSB.append(      compositeNid + "," + headline + "," +  prodCategory +  "," + description + ","          +        "," + purchasing_sash + "," +  cta_url   + ","          +        ","       +      "," +    commonType    + "," +      product_id      + "," +      locale      + "," + purchasing_promotional + "," +   cta_secondary_cta_label    + "," +     cta_secondary_url      + "," + moderation_date + "," +     headline      + "," +     description      + "," +     pre_headline      + "," +      cta_cta_label  + "," +   utlevel10code    + "," + utlevel10description + ","  +   utlevel15code    + ","  + utlevel15description + ","  +  utlevel17code     + ","  + utlevel17description + ","  +  utlevel20code   +   ","  + utlevel20description + ","  +   utlevel30code    + "," + utlevel30description + "," +  DATA_SOURCE    +   "," +   nluKeywords  +  "," +   offeringType +  "\n");
                }
                catch(Exception ex)
                {
                    //Move to next record. Do not stop the entire process. Add failed id to the list.
                    failedNid = failedNid + nid + ",";
                    //ex.printStackTrace();
                }
            }


            //CSV content generation - END



            // Get current Date and Time to append to filename for files going into the S3 archival bucket
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            Date date = new Date();
            String currentDateTime  = formatter.format(date);

            int responseCodeLiveBucket;
            int responseCodeArchiveBucket;
            if (!ENV.equals("PROD")) {
                responseCodeLiveBucket = uploadFileinIBM_COS(s3_live_bucket_csv_url, accessToken, csvSB);
                responseCodeArchiveBucket = uploadFileinIBM_COS(s3_archive_bucket_csv_url + currentDateTime + ".csv", accessToken, csvSB);
            } else {
                System.out.println("PRODUCTION");
                responseCodeLiveBucket = uploadFileinIBM_FTP(DEFAULT_FTP_ENDPOINT_URL, csvSB, DEFAULT_FTP_LIVE_BUCKET_NAME, "drupalReco.csv");
                responseCodeArchiveBucket = uploadFileinIBM_FTP(DEFAULT_FTP_ENDPOINT_URL, csvSB, DEFAULT_FTP_ARCHIVE_BUCKET_NAME, "drupalReco-" + currentDateTime + ".csv");
            }

            responeCode  = "Live Bucket : " +Integer.toString(responseCodeLiveBucket) + " & " + "Archive Bucket : " + Integer.toString(responseCodeArchiveBucket);
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

        try {
            int envIndex = java.util.Arrays.asList(args).indexOf("--ENV");
            ENV = args[envIndex + 1];
            JsonObject response = new JsonObject();
            response.addProperty("live_bucket_name", "map-dev-01/drupal-recomendation/live/");
            response.addProperty("archive_bucket_name", "map-dev-01/drupal-recomendation/archive/");
            response.addProperty("s3_endpoint_url", "https://s3.ap.cloud-object-storage.appdomain.cloud");
            //response.addProperty(DEFAULT_FTP_LIVE_BUCKET_NAME, "map-dev-01/drupal-recomendation/live/");
            //response.addProperty(DEFAULT_FTP_ARCHIVE_BUCKET_NAME, "map-dev-01/drupal-recomendation/archive/");
            //response.addProperty(DEFAULT_FTP_ENDPOINT_URL, "https://s3.ap.cloud-object-storage.appdomain.cloud");
            response.addProperty("drupal_endpoint_url", "https://www.ibm.com/internal/cms/clp/products?_format=json");

            main(response);
            System.out.println("\nAction Completed : " + response);

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

    private static int uploadFileinIBM_FTP(String ftpserver, StringBuilder csvSB, String upLoadPath, String name ) {

        // declare FTP server, credentials, target path and file name
        String server = ftpserver;
        String user = System.getenv("MAP_FTP_USER_ID");
        String pass = System.getenv("MAP_FTP_PASSWORD");
        String uploadPath = upLoadPath;
        String fileName = name;
        int returnCode = 0;

        try {
            // Create FTPS client, connect to ftp server and change working directory
            FTPSClient ftpClient = new FTPSClient();
            System.out.println("Connecting to FTP server");
            ftpClient.connect(server);
            System.out.println("Logging in....");
            //System.out.println(ftpClient.getReplyString());
            boolean login = ftpClient.login(user, pass);
            if (!login) {
                System.out.println("Failed to login..");
                System.out.println(ftpClient.getReplyString());
                ftpClient.logout();
                return 400;
            }
            ftpClient.execPBSZ(0);
            ftpClient.execPROT("P");
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE, FTP.BINARY_FILE_TYPE);
            ftpClient.setFileTransferMode(FTP.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setKeepAlive(true);
            boolean cd = ftpClient.changeWorkingDirectory(uploadPath);
            if (cd) {
                System.out.println("Changed success to " + uploadPath);
            }
            //System.out.println(ftpClient.getReplyString());

            // Create input stream to hold contents of string builder to be uploaded to FTP server
            InputStream inputStream = new ByteArrayInputStream(csvSB.toString().getBytes());

            // Upload the drupal content to the FTP server
            System.out.println("Uploading...");
            boolean done = ftpClient.storeFile(fileName, inputStream);
            String response = ftpClient.getReplyString();
            System.out.println(done + ": :" + response);
            if (done) {
                System.out.println("\n" + fileName + " was uploaded successfully.");
                returnCode = 200;
            }
            else
            { returnCode = 400; }

            // clean up
            inputStream.close();
            ftpClient.logout();
            ftpClient.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return returnCode;
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
        StringBuilder industryKeywords = new StringBuilder();
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
            JsonArray dataJsonArray =    (JsonArray) drupalResponse.get("data");

            for(int i = 0; i < dataJsonArray.size(); i++)
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
                industryKeywords = new StringBuilder();
                utValues = new ArrayList<String>();

                JsonObject umcJsonObj = dataJsonArray.get(i).getAsJsonObject();
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

                    // New Requirement to add "ideal-for-industry" data from UMC

                    if( metadatObj.get("ideal-for-industry")!= null && !metadatObj.get("ideal-for-industry").isJsonNull())
                    {
                        JsonArray industryArray = (JsonArray)metadatObj.get("ideal-for-industry");
                        ArrayList<String> industryArrayList = new ArrayList<String>();
                        String temp = "";

                        for(int j=0; j<industryArray.size(); j++)
                        {
                            temp = industryArray.get(j).getAsString();
                            industryArrayList.add(temp);
                        }

                        if(industryArrayList.size() > 0)
                        {
                            industryKeywords.append("\"[");

                            int industryListSize = industryArrayList.size();

                            for(int k=0; k<industryListSize; k++)
                            {
                                industryKeywords.append("\"\"" + industryArrayList.get(k) + "\"\"");
                                if(k != industryListSize-1)
                                {
                                    industryKeywords.append(",");
                                }
                            }

                            industryKeywords.append("]\"");
                        }
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
                utValues.add(industryKeywords.toString());	// Position 13 - ideal-for-industry
                utValMap.put(productKey, utValues);

            }
        }catch(Exception e) {
            throw new Exception("Unable to retrieve the UT values from UMC");
        }
        return utValMap;

    }

    //Get the Data from the Extract from DR

    private static Map<String,List<String>> getDataFromDR() throws Exception
    {
        Map<String,List<String>> utAndNluMap = new HashMap<String,List<String>> ();
        String accessToken = getToken();
        try {
            URL urlDrupalJson = new URL(DEFAULT_URLS_MISSING_UT_VALUES_LOOKUP_BUCKET_NAME);
            HttpURLConnection connDrupalJsonRead = (HttpURLConnection) urlDrupalJson.openConnection();
            connDrupalJsonRead.setRequestMethod("GET");
            connDrupalJsonRead.setRequestProperty("Authorization", accessToken);
            BufferedReader br = new BufferedReader(new InputStreamReader(connDrupalJsonRead.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                utAndNluMap.put(values[0],(Arrays.asList(values)));
            }
            br.close();
        } catch(Exception ex) {
            throw new Exception("Unable to get UT Values from the CSV. Error :" + ex.getLocalizedMessage());
        }
        return utAndNluMap;
    }

    private static Map<String,List<String>> getPriorityFromCSV() throws Exception
    {
        Map<String,List<String>> priorityMap = new HashMap<String,List<String>> ();
        String accessToken = getToken();
        try {
            URL urlDrupalJson = new URL(PRIORITY_CSV_ENDPOINT);
            HttpURLConnection connDrupalJsonRead = (HttpURLConnection) urlDrupalJson.openConnection();
            connDrupalJsonRead.setRequestMethod("GET");
            connDrupalJsonRead.setRequestProperty("Authorization", accessToken);
            BufferedReader br = new BufferedReader(new InputStreamReader(connDrupalJsonRead.getInputStream()));

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                priorityMap.put(values[0],Arrays.asList(values));
            }
            br.close();
        } catch(Exception ex) {
            throw new Exception("Unable to get Priority/isActive Values from the CSV. Error :" + ex.getLocalizedMessage());
        }

        return priorityMap;
    }
}
