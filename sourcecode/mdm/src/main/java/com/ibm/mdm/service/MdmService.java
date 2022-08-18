package com.ibm.mdm.service;

import java.util.List;

import com.ibm.mdm.bean.ColumnView;
import com.ibm.mdm.bean.SampleData;
import com.ibm.mdm.bean.TableView;

public interface MdmService {
	
    public List<TableView> selectSchemaList();
	
	public List<ColumnView> selectColumnViewList(String name);
	
	public List<TableView> selectTableViewBySchema(String schema);
	
    public Long selectSchemaNum();
	
	public Long selectViewNum();
	
	public Long selectColumnNum();
	
	public List<ColumnView> selectColumnViewListByTable(String tableName);
	
    public List<TableView> selectTableViewByName(String name,String by);
	
	public List<ColumnView> selectColumnViewListByName(String name,String by);

	public String selectLastUpdateDate();

	public List<List<SampleData>> getSampleDataList(String schema,String tableName) throws Exception;

}
