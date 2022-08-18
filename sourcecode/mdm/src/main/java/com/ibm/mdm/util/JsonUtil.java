package com.ibm.mdm.util;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;


import com.alibaba.fastjson.JSONReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class JsonUtil {

    private static String filePath;

    @Value("${sys.sample-data-json-path}")
    public void setFilePath(String path) {
        filePath = path;
    }
	
	public static List<List<String>> getDataFromJson(String tableName) throws Exception{
		String path= filePath+"/"+tableName+".json";
		List<List<String>> result=new ArrayList<List<String>>();
		File file=new File(path);
		if(!file.exists()) {
			return null;
		}
		JSONReader reader = new JSONReader(new FileReader(path));
		reader.startArray();	
		int i=0;
        while (reader.hasNext())
        {
        	List<String> headList=new ArrayList<String>();
    		List<String> valueList=new ArrayList<String>();
            List<String> typeList=new ArrayList<String>();
            reader.startObject();
            while (reader.hasNext())
            {
                String arrayListItemKey = reader.readString();
                String arrayListItemValue = reader.readObject().toString();
                if(i==0) {
                 headList.add(arrayListItemKey);
                 typeList.add(arrayListItemValue);
                }else {
                    valueList.add(arrayListItemValue);
                }
            }
            
           
            reader.endObject();
            if(i==0) {
             result.add(headList);
             result.add(typeList);
            }else {
                result.add(valueList);
            }
            i=i+1;
        }
        reader.endArray();
		return result;
		
	}
	
	/*public static void main(String a[]) {
		
		try {
			JsonUtil.getDataFromJson("V2BMSI2.V_REF_SIW_BP");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}*/

}
