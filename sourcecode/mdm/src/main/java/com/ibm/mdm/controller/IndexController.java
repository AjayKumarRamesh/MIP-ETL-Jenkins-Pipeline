package com.ibm.mdm.controller;

import com.ibm.mdm.bean.*;
import com.ibm.mdm.service.MdmService;
import com.ibm.mdm.util.ExcelUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.thymeleaf.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Controller
public class IndexController extends BaseController{
	Log logger= LogFactory.getLog(this.getClass());

	@Autowired
	MdmService mdmService;

	
	@RequestMapping("/index")
	public String index(Model  model){
		//request.getSession().setAttribute(SessionKeys.SESSION_USER_ID_KEY,"wrrong@cn.ibm.com");
	    String userId=(String)session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
		if(StringUtils.isEmpty(userId)){
			return "redirect:/w3Login";
		}
		try {
			Long schemaNum = mdmService.selectSchemaNum();
			Long viewNum = mdmService.selectViewNum();
			Long columnNum = mdmService.selectColumnNum();
			List<TableView> tableList = mdmService.selectSchemaList();
			String updateDateStr=mdmService.selectLastUpdateDate();
			model.addAttribute("schemaNum", schemaNum);
			model.addAttribute("viewNum", viewNum);
			model.addAttribute("columnNum", columnNum);
			model.addAttribute("tableList", tableList);
			model.addAttribute("updateDateStr", updateDateStr);
		}catch (Exception e){
           logger.error(e.getMessage());
		}
		return "index";		
	}
	
	@RequestMapping("/searchView")
	public String searchView(Model  model,String schema,String schemaName) {
	    String userId=(String)session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
//		if(StringUtils.isEmpty(userId)){
//			return "redirect:/w3Login";
//		}
		List<TableView> tableList=null;
		try {
			if (!StringUtils.isEmpty(schema)) {
				tableList = mdmService.selectTableViewBySchema(schema);
			}
			model.addAttribute("schema", schemaName);
			model.addAttribute("tableList", tableList);
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		return "search_view";		
	}
	
	@RequestMapping("/detail")
	public String detail(Model  model,String tb,String schema,String tbid) {
		String userId=(String)session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
//		if(StringUtils.isEmpty(userId)){
//			return "redirect:/w3Login";
//		}
		List<ColumnView> columnList=null;
		List<List<String>> sampleData=null;
		List<List<SampleData>> sdList=null;
		List<String> headList=new ArrayList<String>();
		try {
			if (!StringUtils.isEmpty(tb)) {
				columnList = mdmService.selectColumnViewListByTable(tbid);
			}
			if (!StringUtils.isEmpty(tb) && !StringUtils.isEmpty(schema)) {
				try {
					sdList=mdmService.getSampleDataList(schema,tb);
					if(sdList!=null){
						if(sdList.size()>0){
							List<SampleData> list=sdList.get(0);
							for(SampleData s:list){
								headList.add(s.getHead());
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			model.addAttribute("tableName", tb);
			model.addAttribute("schema", schema);
			model.addAttribute("columnList", columnList);
			model.addAttribute("headList", headList);
			model.addAttribute("sdList", sdList);
		}catch (Exception e){
			logger.error(e.getMessage());
		}
		return "detail";		
	}
	
	
	@RequestMapping("/columnview")
	public String columnview(Model  model,String name) {
		String userId=(String)session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
//		if(StringUtils.isEmpty(userId)){
//			return "redirect:/w3Login";
//		}
		try {
			List<ColumnView> columnList = mdmService.selectColumnViewList(name);
			model.addAttribute("columnList", columnList);
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		return "columnview";		
	}
	
	@RequestMapping("/search")
	public String search(Model  model,String type,String q,String by) {
		String userId=(String)session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
//		if(StringUtils.isEmpty(userId)){
//			return "redirect:/w3Login";
//		}
		try {
			if (StringUtils.isEmpty(type)) {
				type = "c";
			}
			if (StringUtils.isEmpty(q)) {
				q = "URN";
			} else {
				q = q.toUpperCase();
			}
			if (StringUtils.isEmpty(by)) {
				by = "1";
			}
			q=q.trim();
			model.addAttribute("q", q);
			model.addAttribute("type", type);
			model.addAttribute("by", by);
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		return "search";		
	}
	
	@RequestMapping("/searchList")
	public String searchList(Model  model,String q,String type,String by) {
		String userId=(String)session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
//		if(StringUtils.isEmpty(userId)){
//			return "redirect:/w3Login";
//		}
		List<TableView> tableList=null;
		List<ColumnView> columnList=null;
		try {
			if (StringUtils.isEmpty(type)) {
				type = "v";
			}
			if (StringUtils.isEmpty(q)) {
				q = "URN";
			} else {
				q = q.toUpperCase();
			}
			if (StringUtils.isEmpty(by)) {
				by = "1";
			}
			q=q.trim();
			model.addAttribute("q", q);
			model.addAttribute("type", type);
			model.addAttribute("by", by);
			if (type.equals("v")) {
				tableList = mdmService.selectTableViewByName(q,by);
				model.addAttribute("tableList", tableList);
				return "tableview";
			} else if (type.equals("c")) {
				columnList = mdmService.selectColumnViewListByName(q,by);
				model.addAttribute("columnList", columnList);
				return "columnview";
			}
		}catch (Exception e){
			logger.error(e.getMessage());
		}
		return "tableview";		
	}

	@RequestMapping("/changeLog")
	public String changeLog(Model  model) {
		String userId=(String)session.getAttribute(SessionKeys.SESSION_USER_ID_KEY);
//		if(StringUtils.isEmpty(userId)){
//			return "redirect:/w3Login";
//		}
		try {
			List<ChangeLog> logList = ExcelUtil.getChangeLogs();
			model.addAttribute("logList", logList);
		}catch (Exception e){
			e.printStackTrace();
			logger.error(e.getMessage());
		}

		return "changelog";
	}
	

}
