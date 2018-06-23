package com.zqc.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;

import com.zqc.bean.DominDay;


public class SearchDataImpl  {

	public static void main(String[] args) {
		String start0 = "20150506";
		String end0 = "20150528";
		String domain0 = "python";
		String behavior0 = "";
		String articleId0 = "D0863227";//"D0863227";

		String time = null;
		String articleId = null;
		String domain = null;
		String behavior = null;
		String num = null;
		String percent = null;

		ArrayList<DominDay> list = showAllData(start0, end0, domain0, behavior0, articleId0);
		DominDay dominDay = new DominDay();
		Iterator<DominDay> it = list.iterator();
		while (it.hasNext()) {
			dominDay = it.next();
			time = dominDay.getTime();
			articleId = dominDay.getArticleId();
			domain = dominDay.getDomain();
			behavior = dominDay.getBehavior();
			num = dominDay.getNum();
			if(num!=null) {
				percent = dominDay.getPercent();
			}

			System.out.println(time + "\t" + articleId + "\t" + domain + "\t" + behavior + "\t" + num + "\t" + percent);
		}
	}

	public static String getSql(String start, String end, String domain, String behavior, String articleId) {
		String strSql =  "SELECT * FROM Article_Info WHERE time BETWEEN '" + start + "' AND '" + end + "' AND domain = '" + domain + "' ";
		if(behavior!="") {
			strSql = strSql + "AND behavior = '" + behavior;
		}
		if(articleId!="") {
			strSql = strSql + "AND articleId = '" + articleId;
		}
		strSql = strSql + "' ORDER BY time, num ASC;";
		return strSql;

	}

	public static ArrayList<DominDay> executeSql(String sql) {
        /*DBConnection db = new DBConnection();*/
		Connection conn = DBConnection.getConnection();
        ArrayList<DominDay> list = new ArrayList<DominDay>();
        try {
        	Statement stmt = (Statement) conn.createStatement();
        	ResultSet rs = (ResultSet) stmt.executeQuery(sql);
			while(rs.next()){
				DominDay dominDay = new DominDay();
				dominDay.setTime(rs.getString("time"));
				dominDay.setArticleId(rs.getString("articleId"));
			    dominDay.setDomain(rs.getString("domain"));
			    dominDay.setBehavior(rs.getString("behavior"));
			    dominDay.setNum(rs.getString("num"));
			    list.add(dominDay);
			}
	       //System.out.println(list.size());
           rs.close();
			DBConnection.closeConnection();//�ر�����
       } catch (SQLException e) {
           e.printStackTrace();
       } 
       //System.out.println("sql data num :" + list.size());
       return list;
	}




	public  static ArrayList<DominDay> showAllData(String start0, String end0, String domain0, String behavior0, String articleId0) {
				

		String strSql = getSql( start0, end0, domain0, behavior0, articleId0);
		ArrayList<DominDay> list = executeSql(strSql);
		if(behavior0!=null) {
			getPercent(list);
		}
		return list;
	}

	public static void getPercent(ArrayList<DominDay> list) {
		DominDay dominDay = new DominDay();
		Integer totalCount = 0;
		String num = "";
		for (int i = 0; i < list.size(); i++) {
		    dominDay = list.get(i);
			num = dominDay.getNum();
			if(num!=null) {
				totalCount += Integer.parseInt(num);
			}
		}
		//System.out.println("-----total count ------"+totalCount);
		
 
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMaximumFractionDigits(2);
        String percent;
		for (int i = 0; i < list.size(); i++) {
		    dominDay = list.get(i);
			num = dominDay.getNum();
			if(num!=null) {
				percent = numberFormat.format((float) Integer.parseInt(num) / (float) totalCount * 100) + "%";
				dominDay.setPercent(percent);
				//System.out.println("-----total percent ------"+percent);
			}
		}
	}
}
