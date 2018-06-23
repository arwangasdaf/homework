var basePath;
$(function () {
    var location = (window.location + '').split('/');
    basePath = location[0] + '//' + location[2] + '/' + location[3];
    console.log("basepathe: " + basePath);
});
$(document).ready(function () {
    $("#sub").click(function () {
        var list = document.getElementById("tt");
        var startTime = $("#startTime").val();
        var endTime = $("#endTime").val();
        var domain = $("#domain").val();
        var behavior = $("#behavior").val();
        var params = {
            "startTime": startTime,
            "endTime": endTime,
            "domain": domain,
            'behavior': behavior
        };
        var result0 = "";
        /*   url = "/table.do";
           console.log("url: " + url);
           $.post(url, params, function(result) {
               console.log(result);
               for(var i=0;i<result.length;result++){
                   arr = result[i].split(",");
                   result+="<tr><td>"+arr[0]+"</td><td>"+arr[1] +"</td><td>"+arr[2]+"</td><td>"+arr[3]+"</td><td>"+arr[4]+"</td><td>"+arr[5]+"</td></tr>";
               }

           });*/
        if (startTime.length == 0 || endTime.length == 0 || domain.length == 0 || behavior.length == 0) {
            alert("Input can't be empty1")

        }
        else {
            $.ajax({
                url: basePath + "/table.do",
                type: 'post',
                data: {
                    "startTime": startTime,
                    "endTime": endTime,
                    "domain": domain,
                    "behavior": behavior
                },
                success: function (result) {
                    console.log("resultLengh: " + result.length);
                    for (var i = 0; i < result.length; i++) {
                        var arr = result[i].split(",");
                        console.log("arr: " + arr);
                        console.log("arr0: " + arr[0]);
                        result0 += "<tr><td>" + arr[0] + "</td><td>" + arr[1] + "</td><td>" + arr[2] + "</td><td>" + arr[3] + "</td><td>" + arr[4] + "</td><td>" + arr[5] + "</td></tr>";
                    }

                    list.innerHTML = result0;
                },
                error: function (e) {
                    console.log(e.responseText)

                }
            });
        }

    });
});