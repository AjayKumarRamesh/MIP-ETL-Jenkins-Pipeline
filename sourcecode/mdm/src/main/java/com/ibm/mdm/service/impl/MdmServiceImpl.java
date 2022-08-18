package com.ibm.mdm.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.mdm.bean.ApplicationKeys;
import com.ibm.mdm.bean.SampleData;
import com.ibm.mdm.util.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ibm.mdm.bean.ColumnView;
import com.ibm.mdm.bean.TableView;
import com.ibm.mdm.dao.MdmMapper;
import com.ibm.mdm.service.MdmService;

@Service
public class MdmServiceImpl implements MdmService {

	@Autowired
	MdmMapper mdmMapper;
	
	@Override
	public List<TableView> selectSchemaList() {
		// TODO Auto-generated method stub
		return mdmMapper.selectSchemaList();
	}

	@Override
	public List<ColumnView> selectColumnViewList(String name) {
		// TODO Auto-generated method stub
		return mdmMapper.selectColumnViewList(name);
	}

	@Override
	public List<TableView> selectTableViewBySchema(String schema) {
		// TODO Auto-generated method stub
		return mdmMapper.selectTableViewBySchema(schema);
	}

	@Override
	public Long selectSchemaNum() {
		// TODO Auto-generated method stub
		return mdmMapper.selectSchemaNum();
	}

	@Override
	public Long selectViewNum() {
		// TODO Auto-generated method stub
		return mdmMapper.selectViewNum();
	}

	@Override
	public Long selectColumnNum() {
		// TODO Auto-generated method stub
		return mdmMapper.selectColumnNum();
	}

	@Override
	public List<ColumnView> selectColumnViewListByTable(String tableName) {
		// TODO Auto-generated method stub
		return mdmMapper.selectColumnViewListByTable(tableName);
	}
	
	@Override
	public List<TableView> selectTableViewByName(String name,String by) {
		// TODO Auto-generated method stub
		Map searchMap=new HashMap<String,String>();
		String bySchema=null;
		String byName=null;
		String byDesc=null;
		if(by.contains(ApplicationKeys.SEARCH_BY_ALL)){
			bySchema="1";
			byName="1";
			byDesc="1";
		}else{
			if(by.contains(ApplicationKeys.SEARCH_BY_SCHEMA)){
				bySchema="1";
			}
			if(by.contains(ApplicationKeys.SEARCH_BY_NAME)){
				byName="1";
			}
			if(by.contains(ApplicationKeys.SEARCH_BY_DESC)){
				byDesc="1";
			}
		}
		searchMap.put("bySchema",bySchema);
		searchMap.put("byName",byName);
		searchMap.put("byDesc",byDesc);
		searchMap.put("name",name);
		return mdmMapper.selectTableViewByName(searchMap);
	}

	@Override
	public List<ColumnView> selectColumnViewListByName(String name,String by) {
		// TODO Auto-generated method stub
		Map searchMap=new HashMap<String,String>();
		String bySchema=null;
		String byName=null;
		String byDesc=null;
		if(by.contains(ApplicationKeys.SEARCH_BY_ALL)){
			bySchema="1";
			byName="1";
			byDesc="1";
		}else{
			if(by.contains(ApplicationKeys.SEARCH_BY_SCHEMA)){
				bySchema="1";
			}
			if(by.contains(ApplicationKeys.SEARCH_BY_NAME)){
				byName="1";
			}
			if(by.contains(ApplicationKeys.SEARCH_BY_DESC)){
				byDesc="1";
			}
		}
		searchMap.put("bySchema",bySchema);
		searchMap.put("byName",byName);
		searchMap.put("byDesc",byDesc);
		searchMap.put("name",name);
		return mdmMapper.selectColumnViewListByName(searchMap);
	}

	@Override
	public String selectLastUpdateDate(){
		return mdmMapper.selectLastUpdateDate();
	}

	@Override
	public List<List<SampleData>> getSampleDataList(String schema, String tableName) throws Exception{
		List<List<String>> list=JsonUtil.getDataFromJson(schema + "." + tableName);
		List<String> headList=null;
		List<List<String>> valueList=new ArrayList<List<String>>();
		List<String> typeList=null;
		List<List<SampleData>> sdList=new ArrayList<List<SampleData>>();
		if (list != null && list.size() > 0) {
			for(int i=0;i<list.size();i++) {
				if (i == 0) {
					headList = list.get(i);
				} else if (i == 1) {
					typeList = list.get(i);
				} else {
					valueList.add(list.get(i));
				}
			}
			if(valueList.size()>0){
				for(int i=0;i<valueList.size();i++){
					List<SampleData> sds=new ArrayList<SampleData>();
					List<String> values=valueList.get(i);
					for(int j=0;j<values.size();j++){
						SampleData sd=new SampleData();
                        String cValue=values.get(j);
                        String colName=headList.get(j);
                        String type=typeList.get(j);
						sd.setHead(colName);
						sd.setcValue(cValue);
						sd.setcType(type);
						sds.add(sd);
					}
					sdList.add(sds);
				}
			}
		}
		return sdList;
	}

}
