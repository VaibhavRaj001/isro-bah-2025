var app = angular.module("GalApp", ['satServiceService', '720kb.datepicker', 'ngMaterial', 'rzSlider', 'ui.bootstrap']);
app.controller('MainController', ['$scope', 'satService', '$timeout', '$http', '$window', '$filter', function($scope, satService, $timeout, $http, $window, $filter) {
        var query_string = {};
        var query;
        var vars;
        var temp1;

        $scope.visitor_time = new Date();
        $scope.halfValue = 10;
        $scope.value = 6;
        $scope.frames = 'Frames';
        $scope.speed = (20 - $scope.halfValue) * 75;
        $scope.gif = false;
        $scope.AutoLoad = 'This will automatically referesh images at 15 minutes interval If you dont want to refresh images , please unselect the checkbox.';
        $scope.toggleplay = false;


        var key = 'Item';
        var spkey = key;
        var fav = '';
        var slideimages = new Array();
        var istimages = new Array();
        var temp_new_date = new Array();
        var whichimage = 0;
        var temp = 1;
        // Current image
        var copy = 0;
        var tempimages = new Array();
        var timer;
        var timer1;
        var paused=false;
// for (var i = 0; i < vars.length; i++)
// {
// var pair = vars[i].split("=");
// var key = decodeURIComponent(pair[0])
// var value = decodeURIComponent(pair[1]);
// if (typeof query_string[key] === "undefined") {
// query_string[key] = decodeURIComponent(value);
// } else if (typeof query_string[key] === "string") {
// var arr = [query_string[key], decodeURIComponent(value)];
// query_string[key] = arr;
// } else {
// query_string[key].push(decodeURIComponent(value));
// }
//
// }
// if (!query_string.ds)
// {
// $scope.ds = '/gallery';
// }
// else {
//
// $scope.ds = '/gallery/' + query_string.ds;
// }
// fav = getFavourite();
// document.getElementById('test2').style.opacity = '0';
// if (!query_string.date)
// {
// $scope.start_date = getTodaydate();
// $scope.today = getTodaydate();
// }
// else {
// $scope.start_date = query_string.date;
// $scope.today = query_string.date;
// }
//
// if (query_string.count)
// {
// $scope.count = query_string.count;
// } else {
// $scope.count = "8";
// }
// document.getElementById('load-text').style.display = "block";


// $scope.start_date = getTodaydate();
// $scope.today = getTodaydate();

        $window.ctrlDown=false;
        document.addEventListener('keydown',function(evt){
		var e=window.event||evt;
                var key1=e.which||e.keyCode;
// console.log("Key pressed",key1,$scope.toggleplay);
                if(32==key1){
                        window.ctrlDown=true;
                        if($scope.toggleplay==false)
                                {
// console.log('if');
                        	 
                        $scope.stop();
                              document.getElementById("lod_fnt_btn").style.display = "block";
                              document.getElementById("stp_fnt_btn").style.display = "none";
                              
                                                // $scope.toggleplay=$scope.toggleplay;
// console.log('if'+$scope.toggleplay);
                                }
                        else{
 // console.log('else');
                        	  document.getElementById("lod_fnt_btn").style.display = "none";
                              document.getElementById("stp_fnt_btn").style.display = "block";
                        	
                                $scope.play();
                             
// console.log('else'+$scope.toggleplay);
                        }

                }
        },false);
        
       /* document.addEventListener('contextmenu',function(e){
       	e.preventDefault();
                 if($scope.toggleplay==false)
                                    {
                          $scope.stop();
                                  document.getElementById("lod_fnt_btn").style.display = "block";
                                  document.getElementById("stp_fnt_btn").style.display = "none";
                                  
                                                    // $scope.toggleplay=$scope.toggleplay;
    // console.log('if'+$scope.toggleplay);
                                    }
                            else{
   //  console.log('else');
                            	  document.getElementById("lod_fnt_btn").style.display = "none";
                                  document.getElementById("stp_fnt_btn").style.display = "block";
                            	
                                    $scope.play();
                                 
    // console.log('else'+$scope.toggleplay);
                            }

                    
            },false);
*/

        function getParameters() {
            query = query_string = {};
            query = window.location.search.substring(1);
            vars = query.split("&");
            for (var i = 0; i < vars.length; i++)
            {
                var pair = vars[i].split("=");
                var key = decodeURIComponent(pair[0])
                var value = decodeURIComponent(pair[1]);
                if (typeof query_string[key] === "undefined") {
                    query_string[key] = decodeURIComponent(value);
                } else if (typeof query_string[key] === "string") {
                    var arr = [query_string[key], decodeURIComponent(value)];
                    query_string[key] = arr;
                } else {
                    query_string[key].push(decodeURIComponent(value));
                }
            }
        }

        function initVars() {
		// console.log('initVars');
            if (!query_string.ds)
            {
                $scope.ds = '/gallery';
            }
            else {

                $scope.ds = '/gallery/' + query_string.ds;
            }
            fav = getFavourite();
            document.getElementById('test2').style.opacity = '0';
            if (!query_string.date)
            {
		   // console.log('query string not defined');
                $scope.start_date = getTodaydate();
		    // console.log('$scope.start_date'+$scope.start_date);
                $scope.today = getTodaydate();
            }
            else {
		   // console.log('else');
                $scope.start_date = query_string.date;
                $scope.today = query_string.date;
            }

            if (query_string.count)
            {
                $scope.count = query_string.count;
            } else {
              // $scope.count = "8";
            }
            // console.log("initVars"+$scope.count)
            document.getElementById('load-text').style.display = "block";

        }

        $scope.parse_query_string = function()
        {
            if (query_string.prod)
            {
                $scope.FavouriteValue = "Add To Favourite";
                  oc=findalloccurences(query_string.prod,"'*");
       	     if(oc.length>0){
                 query_string.prod=deletechar(query_string.prod,"'",oc)
	     }
                 // console.log('strm',strm)
		satService.getProd($scope, query_string.prod);
            }
            else if (fav) {
                $scope.FavouriteValue = "Remove Favourite";
                satService.getProd($scope, fav);
            }
            else {
                $scope.FavouriteValue = "Add To Favourite";
                satService.getSat($scope);
            }
            // console.log($scope.color);

        };



        $scope.initOnload = function()
        {
          $scope.tablehide;
            getParameters();
            initVars();
            $scope.parse_query_string();
	      if(query_string.prod=="3DIMG_*_L1C_ASIA_MER_BIMG.jpg")
            {window.location = 'index.html?&prod=3DIMG_*_L1C_ASIA_MER_BIMG_V*.jpg'};
       

        }
        
         $scope.sharebrow = function()
        {
            s = 'https://mosdac.gov.in/gallery/index.html?';
// p = window.location.search.substring(1);
            if (!query_string.ds)
            {
                p = '';
            }
            else {

                p = 'ds=' + query_string.ds;
            }
            f_url = s + p + '&prod=' + $scope.product1.pat + '&date=' + $scope.start_date + '&count=' + $scope.count;
             document.getElementById('sharelink').href = f_url;

        }

	function findalloccurences(str,charToSearch){
		console.log('called console')
		const occ=[];
		let index=str.indexOf(charToSearch);

		while(index !== -1){
			 console.log('occurence ',index);
			//alert(index)
			occ.push(index);
			index=str.indexOf(charToSearch,index+1);

		}
		if(occ.length>0){
			for(i=0;i<occ.length;i++)
			{
				console.log(occ[i])
			//	alert(occ[i])
			}
		
		}

		return occ;
	}

	 function deletechar(inp,charToappend,pos){
                const charArray=inp.split('')
                pos.sort((a,b)=> b-a);

		 pos.forEach(p=>{
			 if(p>=0 && p<charArray.length){
			   charArray.splice(p,1)

			 }
		 });
                return charArray.join('')
        }

	function appendchar(inp,charToappend,pos){
		const charArray=inp.split('')
		pos.sort((a,b)=> a-b);
		for(let i=pos.length-1;i>=0;i--){
			const p=pos[i];
			if(p>=0 && p<=charArray.length){
				charArray.splice(p,0,charToappend)
			}else{
				console.log('Invalid position')
			}
		}
		return charArray.join('')
	}

        $scope.share = function()
        {
	    oc=findalloccurences($scope.product1.pat,"*");
	//	alert(oc.length)	
		 if(oc.length>1){
	//		 alert(oc.length)
		    strm=appendchar($scope.product1.pat,"'",oc)
		 }
	   // alert(strm)
            s = 'https://mosdac.gov.in/gallery/index.html?';
 p = window.location.search.substring(1);
            if (!query_string.ds)
            {
                p = '';
            }
            else {

                p = 'ds=' + query_string.ds;
            }
            f_url = s + p + '&prod=' + $scope.product1.pat + '&date=' + $scope.start_date + '&count=' + $scope.count;
            if (navigator.share) {
                // alert("Congrats! Your browser supports Web Share API")

                // navigator.share accepts objects which must have atleast
				// title, text or
                // url. Any text or title or text is possible
                navigator.share({
                    title: "MOSDAC Gallery",
                    text: "MOSDAC Gallery",
                    url: s + p + '&prod=' + strm + '&date=' + $scope.start_date + '&count=' + $scope.count
                })
                        .then(function() {
                            console.log("Shareing successfull")
                        })
                        .catch(function() {
                            console.log("Sharing failed")
                        })

            } else {
               // console.log(f_url);
               // alert("Sorry! Your browser does not support Web Share API");
                window.open(f_url);
            }

        }

        $scope.load = function()
        {

            $scope.theambgColor = {
                "background": "#095cb1",
            }
            $scope.theamColor = {
                "color": "#095cb1",
            }
          // $scope.7657 = false;
          $scope.start_date=$filter('date')(new Date($scope.start_date),'yyyy-MM-dd');
    
          console.log("Start date ",$scope.start_date);
            // document.getElementById("lod_fnt_btn").style.display="none
			// !important";
            // document.getElementById("stp_fnt_btn").style.display="block
			// !important";
            $timeout.cancel(timer);
            var list = null;
            // console.log('product1',$scope.product1);
		    // console.log("date inv:- ",$scope.start_date=="Invalid Date")
		 if($scope.start_date=="Invalid Date")
                    {
               // console.log('if');
                            $scope.start_date = getTodaydate();

                    }
		    // console.log("date val:- ",$scope.start_date=="Invalid Date")
               // console.log('pattern' + pattern + '$scope.start_date' +
				// $scope.start_date + '$scope.count' +
				// $scope.count+'query_string.date'+query_string.date);
            var pattern = $scope.product1.pat;
            if (pattern == "" || pattern == null || pattern == "undefined")
            {
                pattern = $scope.product1;
                $scope.product1 = $scope.getProd($scope.product1);
            }
            if (pattern == "" || pattern == null || $scope.start_date == null || $scope.start_date == null) {
                alert("select the product/start date")
            }
            else {
               // console.log('pattern' + pattern + '$scope.start_date' +
				// $scope.start_date + '$scope.count' +
				// $scope.count+'query_string.date'+query_string.date);
                if(query_string.date!=null)
                	{
			// console.log('load() query string not null');
                //	$scope.start_date=query_string.date;
                	}
                console.log('prod',pattern,'st_date',$scope.start_date,'count',$scope.count);
                $http.post("getImage.php", {'prod': pattern, 'st_date': $scope.start_date, 'count': $scope.count})
                        // .success(function(data) { //Old part not work with
						// angularjs 1.8
                        .then(function(data) {
                            // alert('load');
                            // alert('$scope.start_date'+$scope.start_date);
                            $scope.filenames = data.data[0];
                            // console.log($scope.filenames);
                            $scope.path = data.data[1];
// alert($scope.path + '$scope.path');
                            list = $scope.filenames;
                            // alert(list+'list');
                            tempimages = new Array();
                            // alert('tempimages'+tempimages);
                            tempimages = list.split(',');
                            if (list == '<' || list == 'na')
                            {
                                alert('No image is available for the given date')
                                $scope.slide = "";
                            }
                            else {
                            	$scope.start_temp();
                            }
                            document.getElementById('load-text').style.display = "none";
                        },
                        function error(data)
                        {
                            console.log('error');
                            document.getElementById('load-text').style.display = "block";
                        });
            }
        }

        $scope.addAlert = function() {
            $scope.start_date = getTodaydate();
            var checked = document.getElementById('switch').checked;
            // console.log(' $scope.start_date'+ $scope.start_date);
            if (checked == true) {
               
                $scope.switch = true;
                timer1 = $timeout(function() {
                    $scope.load();
                    $scope.addAlert();
                }, 900000);
            } else {
            
                $scope.switch = false;
                // addFavourite();
                $timeout.cancel(timer1);
            }
        };

        $scope.slidehide = function() {
            $scope.nextbtn = false;
            document.getElementById('set-slider').style.opacity = '1';
        }
        $scope.slideshow = function() {
            document.getElementById('set-slider').style.opacity = '0';
            $scope.nextbtn = true;
        }

        $scope.favourite = function() {
            var msg;
            if ($scope.FavouriteValue == "Add To Favourite") {
                msg = 'added';
                addFavourite();
                loadmsg.style.display = "block";
                $scope.FavouriteValue = "Remove Favourite";
                $scope.AutoLoadmsg = "Favourite Added";
                setTimeout(function() {
                    loadmsg.style.display = "none";
                }, 1000);

            } else {
                msg = 'removed';
                removeFavourite();
                loadmsg.style.display = "block";
                $scope.FavouriteValue = "Add To Favourite"
                $scope.AutoLoadmsg = "Favourite Removed";
                setTimeout(function() {
                    loadmsg.style.display = "none";
                }, 1000);
            }
        }

        function addFavourite() {
            spkey = key;
            var myObj = $scope.product1.pat;
            if (query_string.ds)
            {
                spkey = spkey + query_string.ds;
            }

            localStorage.setItem(spkey, myObj);

        }

        function getFavourite() {
            spkey = key;
            if (query_string.ds)
            {
                spkey = spkey + query_string.ds;
            }
            var val = localStorage.getItem(spkey);
            return val;
        }

        function removeFavourite() {
            spkey = key;
            if (query_string.ds)
            {
                spkey = spkey + query_string.ds;
            }
            localStorage.removeItem(spkey);
        }


        // Called on satellite change
        $scope.getSatSen = function() {
            satService.satChange($scope);
            $scope.load();
        }

// Called upon product type change
        // Fetch product list based on sensor and product type
        $scope.getSenProdt = function() {
            satService.prodtChange($scope);
            $scope.load();
        }

        $scope.getProdt = function() {
            satService.senChange($scope);
            $scope.load();
        }


        $scope.halfValueChanged = function() {
            $scope.speed = (20 - $scope.halfValue) * 75;
            $timeout(function() {
                $scope.value = $scope.halfValue * 1;
            }, 0, true);
        }
        function getTodaydate()
        {
		// console.log('getToday()');
            var d = new Date();
            var dd = d.getDate();
            if (dd < 10) {
                dd = '0' + dd;
            }
            var mm = d.getMonth() + 1;
            if (mm < 10) {
                mm = '0' + mm;
            }
            d = d.getFullYear() + "-" + mm + "-" + dd;
	// console.log('d:'+d);
            return d;
        }
        function getYesdate()
        {
            // alert('getYesdate');
            var d = new Date();
            d.setDate(d.getDate() - 1);
            var dd = d.getDate();
            // alert(dd+'dd');
            if (dd < 10) {
                dd = '0' + dd;
            }
            var mm = d.getMonth() + 1;
            if (mm < 10) {
                mm = '0' + mm;
            }
            d = d.getFullYear() + "-" + mm + "-" + dd;
            // alert(d+'d');
            return d;
        }

        $scope.play=function()
        {
        // paused=false;
        	$scope.theambgColor = {
                    "background": "#095cb1",
                }
                $scope.theamColor = {
                    "color": "#095cb1",
                }
                document.getElementById("lod_fnt_btn").style.display="none";
                document.getElementById("stp_fnt_btn").style.display="block";
                $timeout.cancel(timer);
                $scope.toggleplay = false;
        	$scope.start_temp();
        }

        $scope.stop = function stopCount()
        {
        // console.log("In stop");
        	paused=true;
            $scope.toggleplay = true;
            document.getElementById("lod_fnt_btn").style.display = "block";
            document.getElementById("stp_fnt_btn").style.display = "none";
//            console.log("Pause() whichimage,copy,tempimages.length-1",whichimage,copy,tempimages.length-1);

            if (copy < tempimages.length - 1)
              {
            	 whichimage = copy;
//                  console.log('Stop() loop if case',whichimage,copy);
              }
              else
              {
              	copy = 0;
              	whichimage =tempimages.length - 1;
//                console.log('Stop() loop else case',whichimage,copy);
              }

            $scope.minRangeSlider.minValue = temp_new_date[copy];
            $scope.slide = slideimages[copy].src;
            $('#filename_id').text(istimages[copy]);

//            console.log("Pause() whichimage,copy,tempimages.length-1",whichimage,copy,tempimages.length-1);
            $timeout.cancel(timer);
        }
        
        function convertjuliantoregulardate(jd)
        {
        	var dayCount=[0,31,59,90,120,151,181,212,243,273,304,334];
        	var mn=0;
        	var dn=0;
        	   for (var i = 0; i < dayCount.length; i += 1) {
        		   if(jd<dayCount[i])
        			   {
        			   mn=i;
        			   dn=jd-dayCount[i]-1;
        			   break;
        			   }
        		   else if(jd==dayCount[i]){
        			   mn=i-1;
        			   dn=jd;
        			   break;
        		   }
        		   else
        			   {
        			   continue
        			   }
        	   }
        	var mn=this.getMonth();
        	var dn=this.getDate();
        	var dayOfYear=dayCount[mn]+dn;
        	if(mn>1 && this.isLeapYear) dayOfYear++;
        	return dayOfYear
        }
        
        function convertdate(sDate){
        	DaysOfMonth=new Array(31,28,31,30,31,30,31,31,30,31,30,31);
        	var isLeapYear; //0 leapyear, 1 non leapyear
//        	var sDate;
        	var sDay;
        	var iDay;
        	var sMonth;
        	var iMonth;
        	var sYear;
        	var iYear;
        	var bValid;
        	if(sDate.length==7)
        	{
        		sYear=sDate.substr(0,4);
        		iYear=parseInt(sYear,10);
        		if(iYear%4==0)
        			{
        				isLeapYear=1;
        				DaysOfMonth[1]=29;
        			}
        		else{
        			isLeapYear=0;
    				DaysOfMonth[1]=28;
        		}
//        		console.log('isLeapYear',isLeapYear)
        		sDay=sDate.substr(4,7);
        		iDay=parseInt(sDay,10);
//        		console.log('iDay',iDay)
        		if(!isNaN(iDay)){
        		if((iDay!=0)&&(iDay<=(365+isLeapYear)))
        		{
        			bValid=true;
        			for(iMonth=0;iDay>DaysOfMonth[iMonth];iMonth++)
        				{
        				iDay-=DaysOfMonth[iMonth];
        				}
        			iMonth++;
        			if(iMonth<10) sMonth="0"+String(iMonth);
        			else sMonth=String(iMonth);
        			if(iDay<10) sDay="0"+String(iDay);
        			else sDay=String(iDay);
        			
        	//			console.log(sDay+"-"+sMonth+"-"+sYear);
        				return(sDay+"-"+sMonth+"-"+sYear);
        		}
        		else{
        			console.log('Invalid date');
        		}
        		}
        		else{
        			console.log('Invalid date');
        		}
        	}
        }
        
       $scope.start_temp =function()
        {
            // console.log('start_temp called');
            // console.log('start temp '+$scope.count);
    	   if(paused==false)
    	   {  whichimage = 0;}
//    	   console.log("Play():paused whichimage",paused,whichimage);
            temp_new_date = new Array();
            slideimages = new Array();
            istimages = new Array();
            var timezone_formal = $scope.visitor_time.getTimezoneOffset();
             console.log('tempimages[whichimage]'+tempimages[whichimage]+'tempimages[whichimage].startsWith("3D");'+tempimages[whichimage].startsWith("3D"));
            var temp_startwith1 = tempimages[whichimage].startsWith("S1");
		var temp_startwith2 =tempimages[whichimage].startsWith("E6_SCT");		
//	console.log("temp_startwith2---",temp_startwith2);	

            // var temp_startwith2 = tempimages[whichimage].startsWith("3R");

            // if(temp_startwith3 === false){
            // document.getElementById('test2').style.opacity = '0';
            // }
            // alert(temp_startwith2)
		//
	    if(tempimages[whichimage].startsWith("E07")){
                    console.log("E07");
                   document.getElementById('test2').style.opacity = '1';
                var proc_level="";
                for (var i = 0; i < tempimages.length; i += temp) {
                    istimages[i] = getString(tempimages[i]);
                     console.log('istimages[i]'+istimages[i]);
                    slideimages[i] = new Image()
                    slideimages[i].src = '/look/' + tempimages[i]

                    temp_str_new1 = getString(tempimages[i])  // Actual date time in GMT with file name portion
                    temp_str_new_temp = temp_str_new1.slice(15, 31)
                    temp_date1 = new Date(temp_str_new_temp.slice(0,4),parseInt(temp_str_new_temp.slice(4,6))-1,temp_str_new_temp.slice(6,8),temp_str_new_temp.slice(8,10),temp_str_new_temp.slice(10,12),temp_str_new_temp.slice(12,14));
		    temp_date1.setMinutes(temp_date1.getMinutes() - timezone_formal)
                    istimages[i] = temp_date1.toDateString()+" "+temp_date1.toTimeString().split(' ')[0]+' ('+temp_date1.toTimeString().split('(')[1];
                 // add 5 hours to gmt date
                    var date = ("0" + (temp_date1.getDate())).slice(-2);
                    var month = ("0" + (temp_date1.getMonth() + 1)).slice(-2);
                    var year = temp_date1.getFullYear();
                    var hr = ("0" + (temp_date1.getHours())).slice(-2);
                    var mn = ("0" + (temp_date1.getMinutes())).slice(-2);
                    new_date = date + '-' + month + '-' + year + '_' + hr + ':' + mn;
                    temp_new_date[i] = new_date;

                }
            }else if(tempimages[whichimage].startsWith("E6_OCM")&&tempimages[whichimage].includes("E06OCML2")){
		    //console.log("OCML2"+tempimages[whichimage]);
                   document.getElementById('test2').style.opacity = '1';
                var proc_level="";
                for (var i = 0; i < tempimages.length; i += temp) {
                    istimages[i] = getString(tempimages[i]);
                     console.log('istimages[i]'+istimages[i]);
                    slideimages[i] = new Image()
                    slideimages[i].src = '/look/' + tempimages[i]

                    temp_str_new1 = getString(tempimages[i])  // Actual date time in GMT with file name portion
                    temp_str_new_temp = temp_str_new1.slice(11, 19)
                    temp_str_new_temp1 = temp_str_new1.slice(20, 26)
		    temp_str_new_temp=temp_str_new_temp+temp_str_new_temp1
                    temp_date1 = new Date(temp_str_new_temp.slice(0,4),parseInt(temp_str_new_temp.slice(4,6))-1,temp_str_new_temp.slice(6,8),temp_str_new_temp.slice(8,10),temp_str_new_temp.slice(10,12),temp_str_new_temp.slice(12,14));
                    temp_date1.setMinutes(temp_date1.getMinutes() - timezone_formal)
                    istimages[i] = temp_date1.toDateString()+" "+temp_date1.toTimeString().split(' ')[0]+' ('+temp_date1.toTimeString().split('(')[1];
                 // add 5 hours to gmt date
                    var date = ("0" + (temp_date1.getDate())).slice(-2);
                    var month = ("0" + (temp_date1.getMonth() + 1)).slice(-2);
                    var year = temp_date1.getFullYear();
                    var hr = ("0" + (temp_date1.getHours())).slice(-2);
                    var mn = ("0" + (temp_date1.getMinutes())).slice(-2);
                    new_date = date + '-' + month + '-' + year + '_' + hr + ':' + mn;
                    temp_new_date[i] = new_date;

                }
            }else if(tempimages[whichimage].startsWith("E6_OCM")){
                    //console.log("OCMOther"+tempimages[whichimage]);
                   document.getElementById('test2').style.opacity = '1';
                var proc_level="";
                for (var i = 0; i < tempimages.length; i += temp) {
                    istimages[i] = getString(tempimages[i]);
                     console.log('istimages[i]'+istimages[i]);
                    slideimages[i] = new Image()
                    slideimages[i].src = '/look/' + tempimages[i]

                    temp_str_new1 = getString(tempimages[i])  // Actual date time in GMT with file name portion
                    temp_str_new_temp = temp_str_new1.slice(11, 19)
                    temp_date1 = new Date(temp_str_new_temp.slice(0,4),parseInt(temp_str_new_temp.slice(4,6))-1,temp_str_new_temp.slice(6,8),temp_str_new_temp.slice(8,10),temp_str_new_temp.slice(10,12),temp_str_new_temp.slice(12,14));
                    temp_date1.setMinutes(temp_date1.getMinutes() - timezone_formal)
                    istimages[i] = temp_date1.toDateString()+" "+temp_date1.toTimeString().split(' ')[0]+' ('+temp_date1.toTimeString().split('(')[1];
                 // add 5 hours to gmt date
                    var date = ("0" + (temp_date1.getDate())).slice(-2);
                    var month = ("0" + (temp_date1.getMonth() + 1)).slice(-2);
                    var year = temp_date1.getFullYear();
                    var hr = ("0" + (temp_date1.getHours())).slice(-2);
                    var mn = ("0" + (temp_date1.getMinutes())).slice(-2);
                    new_date = date + '-' + month + '-' + year + '_' + hr + ':' + mn;
                    temp_new_date[i] = new_date;

                }
            }



		else if (temp_startwith1 === false  &&  temp_startwith2 === false) {

                for (var i = 0; i < tempimages.length; i += temp) {
                    slideimages[i] = new Image()
                    slideimages[i].src = '/look/' + tempimages[i]
//			console.log("slideimage---",tempimages[i]);
                    temp_str_new1 = getString(tempimages[i])  // Actual date time in GMT with file name portion
/*			if(temp_str_new1.startsWith("E07"))
			{
				  temp_str_new_temp = temp_str_new1.slice(15, 31) // Slice to get Date and Time only

			}else{*/
                    temp_str_new_temp = temp_str_new1.slice(6, 20) // Slice to get Date and Time only                         
//			}
                    // console.log(temp_str_new_temp,' check name GST')
                    // Dictionary for month to int month
                    temp_mon_dict1 = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}  
                 // Dictionary for int month to str month
                    temp_mon_dict2 = {1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"} 
                    temp_str_new_temp_dt = parseInt(temp_str_new_temp.slice(0, 2)) // var to store date
                    temp_str_new_temp_mon = temp_mon_dict1[temp_str_new_temp.slice(2, 5)] - 1 // var to store month
                    temp_str_new_temp_yr = parseInt(temp_str_new_temp.slice(5, 9)) // var to store year
                    
                    // To handly DAILY product of Kalpana/INsat-3A
                      if(temp_str_new_temp.includes("DAIL"))
                    	{
	                        temp_str_new_temp_hr=00;
	                    	temp_str_new_temp_mn=00;
                    	}
                      else{
/*			                              if(temp_str_new1.startsWith("E07"))
                        {
                                   temp_str_new_temp_hr = parseInt(temp_str_new_temp.slice(9, 11)) // var to store hour
                          temp_str_new_temp_mn = parseInt(temp_str_new_temp.slice(11, 13)) // var to store minute
 // Slice to get Date and Time only

                        }else{*/

                    	  temp_str_new_temp_hr = parseInt(temp_str_new_temp.slice(10, 12)) // var to store hour
                    	  temp_str_new_temp_mn = parseInt(temp_str_new_temp.slice(12, 14)) // var to store minute
		//	}
                      }
                 // Create date object with value of date in gmt
                    temp_date1 = new Date(temp_str_new_temp_yr, temp_str_new_temp_mon, temp_str_new_temp_dt, temp_str_new_temp_hr, temp_str_new_temp_mn) 
                 // add 5 hours to gmt date
                    temp_date1.setMinutes(temp_date1.getMinutes() - timezone_formal) 
                    var temp_suffix = temp_str_new1.substring(20, temp_str_new1.length);
                    // istimages[i] = temp_date1;
                    istimages[i] = temp_date1.toDateString()+" "+temp_date1.toTimeString().split(' ')[0]+' ('+temp_date1.toTimeString().split('(')[1];
                 // add 5 hours to gmt date
                    var date = ("0" + (temp_date1.getDate())).slice(-2); 
                    var month = ("0" + (temp_date1.getMonth() + 1)).slice(-2);
                    var year = temp_date1.getFullYear();
                    var hr = ("0" + (temp_date1.getHours())).slice(-2);
                    var mn = ("0" + (temp_date1.getMinutes())).slice(-2);
                    new_date = date + '-' + month + '-' + year + '_' + hr + ':' + mn;
                    temp_new_date[i] = new_date;
                }
                document.getElementById('test2').style.opacity = '1';
            } else if(temp_startwith1 === true){
//            	 document.getElementById('test2').style.opacity = '0';
                 var proc_level="";
                 for (var i = 0; i < tempimages.length; i += temp) {
                     istimages[i] = getString(tempimages[i]);
                     // console.log('istimages[i]'+istimages[i]);
                     slideimages[i] = new Image()
                     slideimages[i].src = '/look/' + tempimages[i]
                     
                     temp_str_new1 = getString(tempimages[i]);
                     proc_level=temp_str_new1.slice(2, 4);
                     var d;
                     var milisec;
                     var year;
//                     console.log(proc_level,'proc_level')
                     if(proc_level==="L4"){
                    	 console.log(temp_str_new1);
                    	 //Case1 (for INDIA, GLOBAL,NPOLAR72,SPOLAR72)
//                    	 if(temp_str_new1.match(/S1L4SH[A-Za-z0-9_.]/g)!=null)
                    	 if(temp_str_new1.match(/IN/g)||temp_str_new1.match(/GL/g)||temp_str_new1.match(/DES_NP/g)||temp_str_new1.match(/ASC_SP/g))
                    	 {
                    		 console.log('case1')
                    		 temp_str_new_temp = temp_str_new1.slice(15, 22) // Slice to get Julian Date  
                     	 }
                    	 //Case2(for rest)
                    	 else{
                    		// console.log('case2',temp_str_new_temp,'temp_str_new1',temp_str_new1);
                    		 temp_str_new_temp = temp_str_new1.slice(7, 14) // Slice to get Julian Date  
 	                	 }
                     }else{
                    	 console.log('L3');
                     	temp_str_new_temp = temp_str_new1.slice(6, 13) // Slice to get Julian Date  
                     	
                     }
                     temp_new_date[i] = convertdate(temp_str_new_temp);
                 	 temp_date1 = new Date(temp_new_date[i].substr(6,10),parseInt(temp_new_date[i].substr(3,5),10)-1,temp_new_date[i].substr(0,2)); 
                     istimages[i] = temp_date1.toDateString()+'(Greenwich Mean Time)';
//                     console.log('new_date',temp_new_date[i])
                 }
            }else if(temp_startwith2 === true){
//		    console.log("in temp_startwith2");
                document.getElementById('test2').style.opacity = '1';
                var proc_level="";
                for (var i = 0; i < tempimages.length; i += temp) {
                    istimages[i] = getString(tempimages[i]);
                    // console.log('istimages[i]'+istimages[i]);
                    slideimages[i] = new Image()
                    slideimages[i].src = '/look/' + tempimages[i]
                    
                    temp_str_new1 = getString(tempimages[i])  // Actual date time in GMT with file name portion
                    proc_level=temp_str_new1.slice(6, 8);
                    var d;
                    var milisec;
                    var year;
                    if(proc_level==="L4"){
                    	 if(temp_str_new1.match(/IN/g)||temp_str_new1.match(/GL/g)||temp_str_new1.match(/DES_NP/g)||temp_str_new1.match(/ASC_SP/g))
                    	 {
                    		 console.log('case1')
                    		 temp_str_new_temp = temp_str_new1.slice(19, 26) // Slice to get Julian Date  
			         temp_new_date[i] = convertdate(temp_str_new_temp);
        	                 temp_date1 = new Date(temp_new_date[i].substr(6,10),parseInt(temp_new_date[i].substr(3,5),10)-1,temp_new_date[i].substr(0,2));
	        	         istimages[i] = temp_date1.toDateString()+'(Greenwich Mean Time)';

                     	 }
                    	 //Case2(for rest)
			    else if(temp_str_new1.match(/AH/g)){
				 console.log(temp_str_new1)
	                         temp_str_new_temp_hr = parseInt(temp_str_new1.slice(21, 23)) // var to store hour
	                         temp_str_new_temp_mn = 0 // var to store minute
				 temp_str_new_temp=temp_str_new1.slice(11, 18);
				    // console.log(temp_str_new_temp_hr,temp_str_new_temp_mn)
                 		  temp_new_date[i] = convertdate(temp_str_new_temp);
                                 temp_date1 = new Date(temp_new_date[i].substr(6,10),parseInt(temp_new_date[i].substr(3,5),10)-1,temp_new_date[i].substr(0,2),temp_str_new_temp_hr, temp_str_new_temp_mn);

				    // add 5 hours to gmt date
	                    temp_date1.setMinutes(temp_date1.getMinutes() - timezone_formal)
                    // istimages[i] = temp_date1;
        	            istimages[i] = temp_date1.toDateString()+" "+temp_date1.toTimeString().split(' ')[0]+' ('+temp_date1.toTimeString().split('(')[1];
                 // add 5 hours to gmt date
                	    var date = ("0" + (temp_date1.getDate())).slice(-2);
	                    var month = ("0" + (temp_date1.getMonth() + 1)).slice(-2);
        	            var year = temp_date1.getFullYear();
                	    var hr = ("0" + (temp_date1.getHours())).slice(-2);
	                    var mn = ("0" + (temp_date1.getMinutes())).slice(-2);
        	            new_date = date + '-' + month + '-' + year + '_' + hr + ':' + mn;
                	    temp_new_date[i] = new_date;
 
//				 console.log('AH new_date',temp_date1,istimages[i])



			    }
                    	 else{
                    		 
                    		 temp_str_new_temp = temp_str_new1.slice(11, 18) // Slice to get Julian Date  
                    		 console.log('case2',temp_str_new_temp,'temp_str_new1',temp_str_new1);
		                 temp_new_date[i] = convertdate(temp_str_new_temp);
	                         temp_date1 = new Date(temp_new_date[i].substr(6,10),parseInt(temp_new_date[i].substr(3,5),10)-1,temp_new_date[i].substr(0,2));
        	                 istimages[i] = temp_date1.toDateString()+'(Greenwich Mean Time)';

 	                	 }
	                   
                    }else{
                    	temp_str_new_temp = temp_str_new1.slice(10, 17) // Slice to get Julian Date  
			     temp_new_date[i] = convertdate(temp_str_new_temp);
                                 temp_date1 = new Date(temp_new_date[i].substr(6,10),parseInt(temp_new_date[i].substr(3,5),10)-1,temp_new_date[i].substr(0,2));
                                 istimages[i] = temp_date1.toDateString()+'(Greenwich Mean Time)';

                     }
//                     temp_new_date[i] = convertdate(temp_str_new_temp);
//                	 temp_date1 = new Date(temp_new_date[i].substr(6,10),parseInt(temp_new_date[i].substr(3,5),10)-1,temp_new_date[i].substr(0,2)); 
//                     istimages[i] = temp_date1.toDateString()+'(Greenwich Mean Time)';
//                     console.log('new_date',temp_new_date[i])
          
                }
            }
	    
            minValues = temp_new_date;
            maxValues = temp_new_date[$scope.count - 1];
            // console.log(minValues);
            $scope.minRangeSlider = {
                minValue: minValues,
                maxValue: maxValues, options: {
                    floor: minValues, ceil: maxValues,
                    showTicks: true,
                    // ticksArray:ta,
                    stepsArray: temp_new_date,
                    bindIndexForStepsArray: false,
                    onChange: function(sliderId, modelValue, highValue, pointerType, index) {
                        // console.log('index:
						// '+temp_new_date.indexOf($scope.minRangeSlider.minValue));
                        whichimage = temp_new_date.indexOf($scope.minRangeSlider.minValue);

                        $scope.slide = slideimages[whichimage].src;
                        $scope.minRangeSlider.minValue = temp_new_date[whichimage];
                        $('#filename_id').text(istimages[whichimage]);
                        // console.log('whichimage' + whichimage + '
						// $scope.slide' + $scope.slide);
                    },
                }
            };
            if (isMobileDevice() == true && tempimages.length > 16) {

                $scope.minRangeSlider.options.showTicks = 4;

            }
            paused=false;
            slidetemp();
        }

        function slidetemp() {
            // $('#filename_id').text(getString(tempimages[whichimage]));
            // $('#filename_id').text(istimages[whichimage]);
            // $scope.slide = slideimages[whichimage].src;
            var x = document.getElementById("msgs").complete;

            if (x == true)
            {
                document.getElementById('load-text').style.display = "none";
                $scope.slide = slideimages[whichimage].src;
                $scope.minRangeSlider.minValue = temp_new_date[whichimage];
                $('#filename_id').text(istimages[whichimage]);
            }
            else {
                // alert('loading');
                document.getElementById('load-text').style.display = "block";
            }
            if (whichimage < tempimages.length - 1)
            {
            	whichimage += temp;
                copy = whichimage - 1;
//                console.log('loop if case',whichimage,copy);
            }
            else
            {
            	whichimage = 0;
                copy = tempimages.length;
//                console.log('loop else case',whichimage,copy);
            }
            timer = $timeout(slidetemp, $scope.speed);
        }

        function getString(str)
        {
            if (str != null && str != "")
            {
                var str1 = str.split('/');
                var len = str1.length - 1;
                str = str1[len].toString();
                str1 = str.substring(0, str.length - 4);
                return str1.toString();
            }
            else
                return'';
        }
        
      
        $scope.show = function()
        {
            $scope.playbar = true;
        }
        $scope.hide = function()
        {
            $scope.playbar = false;
        }
        $scope.open = function()
        {
            $scope.table = true;
        }
        $scope.tablehide = function()
        {
            $scope.table = false;
        }
        $scope.close = function()
        {
            $scope.table = false;
        }
        $scope.gifopen = function()
        {
            $scope.gif = true;
        }
        $scope.prev_day = function()
        {

            $timeout.cancel(timer);
            if (copy == 0)
            {
                copy = tempimages.length - 1;
                whichimage = 0;
            }
            else
            {
                copy -= 1;
                whichimage = copy - 1;
            }
            $scope.minRangeSlider.minValue = temp_new_date[copy];
            $scope.slide = slideimages[copy].src;
            $('#filename_id').text(istimages[copy]);

        }

        $scope.next_day = function()
        {

            $timeout.cancel(timer);
            if (copy == tempimages.length - 1)
            {
                copy = 0;
            }
            else
                copy += 1;
            $scope.minRangeSlider.minValue = temp_new_date[copy];
            $scope.slide = slideimages[copy].src;
            $('#filename_id').text(istimages[copy]);
            whichimage = copy + 1;
            if (whichimage == tempimages.length)
            {
                whichimage = 0;
            }
        }

        /* For Swiping */
        $scope.swipeLeft = function(ev, scope) {
            // $scope.updatesize = 70;
            var scrinheight = screen.height;
            var scrinwidth = screen.width;
            var scale = zoomController(1, $scope.updatesize) / 70;
            // alert('scale'+scale);
            var ScrinHeight = scrinheight;
            var elem = document.getElementById('msgs');
            var Imgheight = elem.offsetHeight;
            var Imgwidth = elem.offsetWidth;
            var newheight = scale * Imgheight;
            // alert(newheight+'newheight');
            var newwidth = scale * Imgwidth;
            if (ScrinHeight < newheight) {
                // Panning case. Next Product will not be shown
                document.getElementById('msgs').style.position = 'relative';
            } else {
                satService.getNextProd($scope, $scope.product1.pat);
                $scope.pattern = $scope.product1.pat;
                $timeout.cancel(timer);
                $scope.swipeload();
            }

        };
        $scope.swipeRight = function(ev)
        {
            var scrinheight = screen.height;
            var scrinwidth = screen.width;
            var scale = zoomController(1, $scope.updatesize) / 70;
            // alert('scale'+scale);
            var ScrinHeight = scrinheight;
            var elem = document.getElementById('msgs');
            var Imgheight = elem.offsetHeight;
            var Imgwidth = elem.offsetWidth;
            var newheight = scale * Imgheight;
            // alert(newheight+'newheight');
            var newwidth = scale * Imgwidth;
            if (ScrinHeight < newheight) {
                document.getElementById('msgs').style.position = 'relative';
            } else {
                satService.getPrevProd($scope, $scope.product1.pat);
                $scope.pattern = $scope.product1.pat;
                $timeout.cancel(timer);
                $scope.swipeload();
            }

        };

        $scope.swipeload = function()
        {
            // document.getElementById('test1').style.position = 'relative';
            var list = null;
            $http.post("getImage.php", {'prod': $scope.pattern, 'st_date': $scope.start_date, 'count': $scope.count})
                    // .success(function(data) { //Old part not work with
					// angularjs 1.8.2
                    .then(function(data) {
                        // alert('get');
                        $scope.filenames = data.data[0];
                        $scope.path = data.data[1];
                        list = $scope.filenames;
                        // alert($scope.filenames);
                        tempimages = new Array();
                        tempimages = list.split(',');
                        // alert()
                        if (list == '<' || list == 'na')
                        {
                            // alert('No image is available for the given date')
                            $scope.slide = "";
                        }
                        else {
                        	$scope.start_temp();
                        }
                    }
                    ,function error(data)
                    {
                        console.log('error');
                    });

            // }
        }
        function isMobileDevice() {
            return (typeof window.orientation !== "undefined") || (navigator.userAgent.indexOf('IEMobile') !== -1);
        }
        
        /* ===============Added By Jay Start On 11MAR2022====================== */

        $scope.zoomIn=function(){
        	// console.log("In Zoom In");
        	$timeout(function() {
                $scope.updatesize = zoomController(1, $scope.updatesize);
            }, 30);
        }
        
        $scope.zoomOut=function(){
        	// console.log("In Zoom Out");
        	$timeout(function() {
                 $scope.updatesize = zoomController(0, $scope.updatesize);
            }, 30);
        }

        /* ===============Added By Jay End On 11MAR2022====================== */



    }]);

// zoom image
function zoomController(zoomtype, updatesize) {
    if (zoomtype == 1 && updatesize > 800) {
        // document.getElementById('msgs').style.position = 'relative';
        return updatesize;
    } else if (zoomtype === 1 && updatesize < 800) {
        return updatesize * 1.09;
    } else if (zoomtype === 0 && updatesize > 20) {
        return updatesize / 1.09;
    } else {
        return updatesize;
    }
}
// zoom image
app.directive('myDraggable', ['$document', '$timeout', function($document, $timeout) {
        return function(scope, element) {
            var startX = 0,
                    startY = 0,
                    x = 0,
                    y = 0;
            scope.updateX = 1;
            scope.updatesize = 70;

            // document.getElementById('msgs').style.transform = 'scale(1)';
            /* mouse wheel */
            var doScroll = function() {
                e = window.event || e;
                var delta = Math.max(-2, Math.min(1, (e.wheelDelta || -e.detail)));
                // alert(delta+'delta');
                $timeout(function() {
                    if (delta == 1) {
                        scope.updatesize = zoomController(1, scope.updatesize);
                    } else {
                        scope.updatesize = zoomController(0, scope.updatesize);
                    }
                }, 30);
                e.preventDefault();
            };

            if (window.addEventListener) {
                window.addEventListener("mousewheel", doScroll, false);
                window.addEventListener("DOMMouseScroll", doScroll, false);
            } else {
                window.attachEvent("onmousewheel", doScroll);
            }
            /* mouse wheel */
            scope.zoomInImage = function() {

                scope.updatesize = zoomController(2, scope.updatesize);
            };
            scope.zoomOutImage = function() {
               // alert('call');
                scope.updatesize = zoomController(0, scope.updatesize);
            };

            element.on('mousedown', function(event) {
            	
                // Prevent default dragging of selected content
                event.preventDefault();
                startX = event.pageX - x;
                startY = event.pageY - y;
                $document.on('mousemove', mousemove);
                $document.on('mouseup', mouseup);
            });


            function mousemove(event) {
                var scrinheight = screen.height;
                var scrinwidth = screen.width;
                var scale = zoomController(1, scope.updatesize) / 70;
                var ScrinHeight = scrinheight;
                var elem = document.getElementById('msgs');
                var Imgheight = elem.offsetHeight;
                var Imgwidth = elem.offsetWidth;
                var newheight = scale * Imgheight;
                var newwidth = scale * Imgwidth;
                if (ScrinHeight < newheight) {
                    y = event.pageY - startY;
                    x = event.pageX - startX;

                    scope.updateX = x;
                    scope.updateY = y;

                    scope.$apply();

                    element.css({
                        top: y + 'px',
                        left: x + 'px'
                    });
                }
            }
            function mouseup() {
                $document.off('mousemove', mousemove);
                $document.off('mouseup', mouseup);
            }
        };
    }]);
// end zoom image
// pinch-zoom start
app.directive('ngPinchZoom', function() {

    var _directive = {
        restrict: 'A',
        scope: false,
        link: _link
    };
    function _link(scope, element, attrs) {
        // alert('call');
        var elWidth, elHeight;

        // mode : 'pinch' or 'swipe'
        var mode = '';

        // distance between two touche points (mode : 'pinch')
        var distance = 0;
        var initialDistance = 0;

        // image scaling
        var scale = 1;
        var relativeScale = 1;
        var initialScale = 1;
        var maxScale = parseInt(attrs.maxScale, 10);
        // alert(maxScale+'maxScale'+attrs.maxScale+'attrs.maxScale');
        if (isNaN(maxScale) || maxScale <= 1) {
            maxScale = 10;
            // alert('3')
        }

        // position of the upper left corner of the element
        var positionX = 0;
        var positionY = 0;

        var initialPositionX = 0;
        var initialPositionY = 0;

        // central origin (mode : 'pinch')
        var originX = 0;
        var originY = 0;

        // start coordinate and amount of movement (mode : 'swipe')
        var startX = 0;
        var startY = 0;
        var moveX = 0;
        var moveY = 0;

        var image = new Image();
        /*
		 * image.onload = function() { alert('image loaded'); elWidth =
		 * element[0].clientWidth; elHeight = element[0].clientHeight;
		 * 
		 * element.css({ '-webkit-transform-origin' : '0 0', 'transform-origin' :
		 * '0 0'
		 * 
		 * });
		 * 
		 *  };
		 */

        /**
		 * @param {object}
		 *            evt
		 */
        function touchstartHandler(evt) {
            // alert('touchstartHandler');


            var touches = evt.originalEvent ? evt.originalEvent.touches : evt.touches;

            startX = touches[0].clientX;
            startY = touches[0].clientY;
            initialPositionX = positionX;
            initialPositionY = positionY;
            moveX = 0;
            moveY = 0;


        }
        /**
		 * @param {object}
		 *            evt
		 */
        var touchmoveHandler = function touchmoveHandler(evt) {
            // alert('touchmoveHandler');
            // elWidth=element[0].width;
            // elHeight=element[0].height;
            elWidth = element[0].clientWidth;
            elHeight = element[0].clientHeight;

            // alert('elWidth'+elWidth+'elHeight'+elHeight);
            // alert('touch');
            element.css({
                '-webkit-transform-origin': '0 0',
                'transform-origin': '0 0'
            });
            evt = ("#msgs").event || evt;
            // evt.preventDefault();
            // $timeout(function () {
            var touches = evt.originalEvent ? evt.originalEvent.touches : evt.touches;

            if (mode === '') {
                if (touches.length === 1 && scale > 1) {

                    mode = 'swipe';

                } else if (touches.length === 2) {

                    mode = 'pinch';

                    initialScale = scale;
                    initialDistance = getDistance(touches);
                    originX = touches[0].clientX -
                            parseInt((touches[0].clientX - touches[1].clientX) / 2, 10) -
                            element[0].offsetLeft - initialPositionX;
                    originY = touches[0].clientY -
                            parseInt((touches[0].clientY - touches[1].clientY) / 2, 10) -
                            element[0].offsetTop - initialPositionY;

                }
            }
            if (mode === 'swipe') {
                evt.preventDefault();

                moveX = touches[0].clientX - startX;
                moveY = touches[0].clientY - startY;

                positionX = initialPositionX + moveX;
                positionY = initialPositionY + moveY;
                transformElement();
            } else if (mode === 'pinch') {
                evt.preventDefault();

                distance = getDistance(touches);
                relativeScale = distance / initialDistance;
                scale = relativeScale * initialScale;

                positionX = originX * (1 - relativeScale) + initialPositionX + moveX;
                positionY = originY * (1 - relativeScale) + initialPositionY + moveY;

                transformElement();

            }
            // }, 30);
            // evt.preventDefault();

        };

        msgs.addEventListener('touchstart', touchstartHandler);
        msgs.addEventListener('touchmove', touchmoveHandler);
        msgs.addEventListener('touchend', touchendHandler);
        if ((msgs).addEventListener) {
            // alert('attaching event');
            msgs.addEventListener("touchmove", touchmoveHandler, false);
            // msgs.addEventListener("DOMMouseScroll", doScroll, false);
        } else {
            msgs.attachEvent("touchmove", touchmoveHandler);
        }

        // if (attrs.ngSrc) {
        // image.src = attrs.ngSrc;
        // } else {
        // image.src = attrs.src;
        // }

        /**
		 * @param {object}
		 *            evt
		 */
        function touchendHandler(evt) {
            // alert('touchendHandler');
            var touches = evt.originalEvent ? evt.originalEvent.touches : evt.touches;

            if (mode === '' || touches.length > 0) {
                return;
            }

            if (scale < 1) {
                scale = 1;
                positionX = 0;
                positionY = 0;
            } else if (scale > maxScale) {
                scale = maxScale;
                relativeScale = scale / initialScale;
                positionX = originX * (1 - relativeScale) + initialPositionX + moveX;
                positionY = originY * (1 - relativeScale) + initialPositionY + moveY;

            } else {
                if (positionX > 0) {
                    positionX = 0;
                } else if (positionX < elWidth * (1 - scale)) {
                    positionX = elWidth * (1 - scale);
                }
                if (positionY > 0) {
                    positionY = 0;
                } else if (positionY < elHeight * (1 - scale)) {
                    positionY = elHeight * (1 - scale);
                }
            }
            transformElement(0.1);
            mode = '';
        }
        /**
		 * @param {Array}
		 *            touches
		 * @return {number}
		 */
        function getDistance(touches) {
            var d = Math.sqrt(Math.pow(touches[0].clientX - touches[1].clientX, 2) +
                    Math.pow(touches[0].clientY - touches[1].clientY, 2));
            return parseInt(d, 10);
        }
        /**
		 * @param {number}
		 *            [duration]
		 */
        function transformElement(duration) {
            var transition = duration ? 'all cubic-bezier(0,0,.5,1) ' + duration + 's' : '';
            var matrixArray = [scale, 0, 0, scale, positionX, positionY];
            var matrix = 'matrix(' + matrixArray.join(',') + ')';
            element.css({
                '-webkit-transition': transition,
                transition: transition,
                '-webkit-transform': matrix + ' translate3d(0,0,0)',
                transform: matrix
            });
        }
    }

    return _directive;
});
// pinch-zoom end




