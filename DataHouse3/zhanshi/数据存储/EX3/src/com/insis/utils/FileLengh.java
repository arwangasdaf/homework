package com.insis.utils;

/**
 * @author ZhuQichao
 * @create 2018/6/7 12:32
 **/
public enum FileLengh {
    //可以根据文件名获取要存储的字段和长度
    userBasic("Uid,Gender,Status,Level", 4),
    userEdu("Uid,Degree,SchoolName,MajorStr", 4),
    userSkill("Uid,SkillName", 2),
    userInterest("Uid,InterestName", 2),
    userBehavior("Uid,Behavior,Aid,BehaviorTime", 4),
    articleInfo("Aid,Type,IsTop,Status,domain", 5),
    test("Uid,Behavior,Aid,BehaviorTime", 4);


    private int value;


    private String zd;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getZd() {
        return zd;
    }

    public void setZd(String zd) {
        this.zd = zd;
    }

    private FileLengh(int value) {
        this.value = value;
    }

    private FileLengh(String zd, int value) {
        this.zd = zd;
        this.value = value;
    }

    FileLengh() {

    }

}
