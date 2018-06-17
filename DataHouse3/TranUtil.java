package DataHouse3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/*
* @class: TranUtil
* @author: wangtao
* @datetime: 2018-06-16
* @descibe: 这个类用于做一些转换操作
* */
public class TranUtil {

    /*
    * @method: tran: 用于将一个日期转化为第几周
    * @param: today: 日期 eg：2018-06-16
    * @return: String: 周数
    * */
    public static String tran(String today){

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = format.parse(today);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setTime(date);

        System.out.println(calendar.get(Calendar.WEEK_OF_YEAR));

        return ""+calendar.get(Calendar.WEEK_OF_YEAR);
    }
}
