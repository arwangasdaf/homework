package com.insis.bean;


import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author ZhuQichao
 * @create 2018/6/8 15:22
 **/
public class UserBehavior implements Comparable<UserBehavior> {
    private String Uid;
    private String Behavior;
    private String Aid;

    public String getUid() {
        return Uid;
    }

    public void setUid(String uid) {
        Uid = uid;
    }

    public String getBehavior() {
        return Behavior;
    }

    public void setBehavior(String behavior) {
        Behavior = behavior;
    }

    public String getAid() {
        return Aid;
    }

    public void setAid(String aid) {
        Aid = aid;
    }

    public String getBehaviorTime() {
        return BehaviorTime;
    }

    public void setBehaviorTime(String behaviorTime) {
        BehaviorTime = behaviorTime;
    }

    private String BehaviorTime;


    public UserBehavior(){

    }



    public UserBehavior(String Uid, String Behavior, String Aid, String BehaviorTime){
        this.Uid = Uid;
        this.Behavior = Behavior;
        this. Aid = Aid;
        this.BehaviorTime = BehaviorTime;
    }
    public UserBehavior(String line){
        String fields[] = line.split("\001");
        Uid = fields[0];
        Behavior = fields[1];
        Aid = fields[2];
        BehaviorTime = fields[3];
    }
    @Override
    public int compareTo(UserBehavior o) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            if (sdf.parse(o.getBehaviorTime()).before(sdf.parse(this.BehaviorTime))) {
                return 1;
            }else{
                return -1;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }



}
