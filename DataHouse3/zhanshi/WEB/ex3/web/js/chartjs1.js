var basePath;
$(function () {
    var location = (window.location + '').split('/');
    basePath = location[0] + '//' + location[2] + '/' + location[3];
    console.log("basepathe0: " + basePath);
});
test=[20150510,20150511];
num0_test=[3,6];
num1_test=[1,4];
num2_test=[2,5];
init(test,num0_test,num1_test,num2_test);
$(document).ready(function () {

    /*$("form").submit(function (e) {*/
    $("#sub").click(function () {

        // alert("sub");
        var startTime = $("#startTime").val();
        var endTime = $("#endTime").val();
        var domain = $("#domain").val();
        var articleID = $("#articleID").val();

        if (startTime.length == 0 || endTime.length == 0 || domain.length == 0 || articleID.length == 0) {
            alert("Input can't be empty1")

        }
        else {
              $.ajax({
               /* url: basePath+"/chart.do",*/
                  url: "/chart.do",
                type: 'post',
                data: {
                    "startTime": startTime,
                    "endTime": endTime,
                    "domain": domain,
                    "articleID": articleID
                },
                success: function (result) {

                    console.log("resultLengh: " + result.length);

                    var times = [];
                    var num0 = [];
                    var num1 = [];
                    var num2 = [];
                    var num0_count = -1;
                    var num1_count = -1;
                    var num2_count = -1;
                    for (var i = 0; i < result.length; i++) {

                        times[i] = result[i].split(",")[0];
                        console.log(i+": " + result[i].split(",")[1]);
                        if (result[i].split(",")[1] == 0) {
                            num0_count++;
                            num0[num0_count] = result[i].split(",")[2];
                            console.log(" num0[i]: " +  num0[i])
                        }
                        else if (result[i].split(",")[1] == 1) {
                            num1_count++;
                            num1[num1_count] = result[i].split(",")[2];
                            console.log(" num1[i]: " +  num1[i])
                        }
                        else {
                            num2_count++;
                            num2[num2_count] = result[i].split(",")[2];
                            console.log(" num2[i]: " +  num2[i])
                        }


                    }
                    console.log("num0: " + num0.length);
                    console.log("num1: " + num1.length);
                    console.log("num2: " + num2.length);
                    console.log("num2_0: " + num2[0]);
                    console.log("time " + times.length);

                    function rep(arr) {
                        var ret = [];
                        for (var i = 0; i < arr.length; i++) {
                            if (arr.indexOf(arr[i]) == i) {
                                ret.push(arr[i]);
                            }
                        }
                        return ret;
                    }

                    arr2 = rep(times);

                    console.log("arr2 " + arr2.length);
                    // 基于准备好的dom，初始化echarts实例
                    init(arr2,num0,num1,num2);
                },
                  error: function (e) {
                      console.log(e.responseText)

                  }
            });


        }


        // return false;
    });


});
function init( arr2, num0, num1, num2) {
    var myChart0 = echarts.init(document.getElementById('lineChart'));
    var myChart1 = echarts.init(document.getElementById('barChart'));
    option0 = {
        title: {
            text: '折线图堆叠'
        },
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data:['发表文章','浏览文章','评论文章']
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        toolbox: {
            feature: {
                saveAsImage: {}
            }
        },
        xAxis: {
            type: 'category',
            boundaryGap: false,
            /*data: ['周一','周二','周三','周四','周五','周六','周日']*/
            data:arr2
        },
        yAxis: {
            type: 'value'
        },
        series: [
            {
                name:'发表文章',
                type:'line',
                stack: '总量',
                // data:[120, 132, 101, 134, 90, 230, 210]
                data:num0

            },
            {
                name:'浏览文章',
                type:'line',
                stack: '总量',
                // data:[220, 182, 191, 234, 290, 330, 310]
                data:num1
            },
            {
                name:'评论文章',
                type:'line',
                stack: '总量',
                /*data:[150, 232, 201, 154, 190, 330, 410]*/
                data:num2
            }
        ]
    };


    option1 = {
        tooltip : {
            trigger: 'axis',
            axisPointer : {            // 坐标轴指示器，坐标轴触发有效
                type : 'shadow'        // 默认为直线，可选为：'line' | 'shadow'
            }
        },
        legend: {
            data: ['发表文章','浏览文章','评论文章']
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis:  {
            type: 'value'
        },
        yAxis: {
            type: 'category',
            data: arr2
        },
        series: [

            {
                name: '发表文章',
                type: 'bar',
                stack: '总量',
                label: {
                    normal: {
                        show: true,
                        position: 'insideRight'
                    }
                },
                data: num0
            },
            {
                name: '浏览文章',
                type: 'bar',
                stack: '总量',
                label: {
                    normal: {
                        show: true,
                        position: 'insideRight'
                    }
                },
                data: num1
            },
            {
                name: '评论文章',
                type: 'bar',
                stack: '总量',
                label: {
                    normal: {
                        show: true,
                        position: 'insideRight'
                    }
                },
                data: num2
            }
        ]
    };
    myChart0.setOption(option0);
    myChart1.setOption(option1);

}