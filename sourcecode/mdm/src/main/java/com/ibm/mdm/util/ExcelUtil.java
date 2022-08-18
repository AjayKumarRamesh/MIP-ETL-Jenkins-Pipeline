package com.ibm.mdm.util;

import com.ibm.mdm.bean.ChangeLog;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Author  Johnny
 * Create  2018-07-16
 * Desc
 */
@Component
public class ExcelUtil {

    private static String filePath;

    @Value("${sys.change_log_excel_path}")
    public void setFilePath(String path) {
        filePath = path;
    }

    public static List<ChangeLog> getChangeLogs() throws Exception {
        List<ChangeLog> list = new ArrayList<ChangeLog>();
        XSSFWorkbook wb = new XSSFWorkbook(new FileInputStream(filePath));
        XSSFSheet sheet = wb.getSheetAt(0);
        int firstRowNum = sheet.getFirstRowNum() + 1;
        int lastRowNum = sheet.getLastRowNum();
        for (int rIndex = firstRowNum; rIndex <= lastRowNum; rIndex++) {
            Row row = sheet.getRow(rIndex);
            ChangeLog bean = new ChangeLog();
            if (row != null) {
                int firstCellNum = row.getFirstCellNum();
                int lastCellNum = row.getLastCellNum();
                for (int cIndex = firstCellNum; cIndex < lastCellNum; cIndex++) {
                    if (cIndex == firstCellNum) {
                        bean.setPlatform(row.getCell(cIndex).getStringCellValue());
                    } else if (cIndex == firstCellNum + 1) {
                        bean.setViewName(row.getCell(cIndex).getStringCellValue());
                    } else if (cIndex == firstCellNum + 2) {
                        bean.setChangeDesc(row.getCell(cIndex).getStringCellValue());
                    } else if (cIndex == firstCellNum + 3) {
                        bean.setBuildRequest(row.getCell(cIndex).getStringCellValue());
                    } else if (cIndex == firstCellNum + 4) {
                        bean.setRelatedStory(row.getCell(cIndex).getStringCellValue());
                    } else if (cIndex == firstCellNum +5) {
                        bean.setUpdateDate(row.getCell(cIndex).getDateCellValue());
                    }
                }
                list.add(bean);
            }
        }
        return list;
    }

   /* public static void main(String a[]) {

		try {
            List<ChangeLog> list=ExcelUtil.getChangeLogs();
            System.out.println(list.size());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}*/
}
