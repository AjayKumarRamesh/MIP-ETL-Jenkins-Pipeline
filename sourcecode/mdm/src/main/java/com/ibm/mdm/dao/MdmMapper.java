package com.ibm.mdm.dao;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.ibm.mdm.bean.ColumnView;
import com.ibm.mdm.bean.TableView;
import java.util.Map;

@Mapper
public interface MdmMapper {
	
	public List<TableView> selectSchemaList();
	
	public List<ColumnView> selectColumnViewList(String name);
	
	public List<TableView> selectTableViewBySchema(String schema);
	
	public Long selectSchemaNum();
	
	public Long selectViewNum();
	
	public Long selectColumnNum();
	
	public List<ColumnView> selectColumnViewListByTable(String tableName);

	public List<TableView> selectTableViewByName(Map<String,String> map);
	
	public List<ColumnView> selectColumnViewListByName(Map<String,String> map);

	public String selectLastUpdateDate();

}
