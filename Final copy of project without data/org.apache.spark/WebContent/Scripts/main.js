function addLoadEvent(func) {    
	var oldonload = window.onload;  
	if (typeof window.onload != 'function') {
		window.onload = func;  
	} else {    
		window.onload = function() {      
			oldonload();      
			func();    }  
	}
}

//addLoadEvent(bannerClick);

function bannerClick() {

	fields = document.getElementById("formLogin").elements;
	for (i=0; i < (fields.length)-1; i++) {
		/*if (fields[i].type == "password") {
			fields[i].type = 'text';
		}*/
		if (fields[i].type == "text" || fields[i].type=="password") {
			fields[i].value = fields[i].id;
			if (window.addEventListener) {
				fields[i].addEventListener("focus", removeBanner, false);
				fields[i].addEventListener("blur", checkField, false);
			} else {
				fields[i].attachEvent("onfocus", removeBanner);
				fields[i].attachEvent("onblur", checkField);
			}
		}
	}
	banner = document.getElementById("aCornerOverlay");
	if (window.addEventListener) {
		banner.addEventListener("click", checkBanner, false);
	} else {
		banner.attachEvent("onClick", checkBanner);
	}
}

function screenPop(img) {
	window.open("/www/images/screenshots/"+img, "menubar=no, location=no, status=no, height=500, width=500");
}	

function removeBanner(f) {
	o = f.target ? f.target : f.srcElement ? f.srcElement : null;
	if (o.value == o.id) {
		o.value = '';
	}
	document.getElementById('divCornerOverlay').style.display = 'none';
}

function checkField(f) {
	o = f.target ? f.target : f.srcElement ? f.srcElement : null;
	if (o && o.value == '') {
		o.value = o.id;
	}
}

function checkBanner() {
  document.location.href = "/trial";
}

/***********************************************
* Switch Menu script- by Martial B of http://getElementById.com/
* Modified by Dynamic Drive for format & NS4/IE4 compatibility
* Visit http://www.dynamicdrive.com/ for full source code
***********************************************/

// initializes display by hiding submenus

function resetMenu(obj){
	if(document.getElementById){
	var el = document.getElementById(obj);
	var ar = document.getElementById("ulSubMenu").getElementsByTagName("ul"); //DynamicDrive.com change
		if(el.style.display != "block"){ //DynamicDrive.com change
			for (var i=0; i<ar.length; i++){
				if (ar[i].className=="ulSecondSub") { //DynamicDrive.com change
					ar[i].style.display = "none";
				}
			}
			el.style.display = "block";
		} else {
			el.style.display = "none";
		}
	}
}

function InitRibbon() {
  // Body onLoad Function 
  
  // Check to see if trial cookie is enabled
  
  var isTrialCookieSet = readCookie("FreeTrial");
  if (isTrialCookieSet) {
    
    // Hide the Pink Ribbon
    document.getElementById('divCornerOverlay').style.display = 'none';
    
    /*
    // Swap Text Field Back to Password Field
    fields = document.getElementById("formLogin").elements;
	  for (i=0; i < (fields.length)-1; i++) {
		  if (fields[i].name == "password") {
			  fields[i].type = 'password';
			}		
		}	
		
		*/		  
  }
  else { 
    // Show the Ribbon
    document.getElementById('divCornerOverlay').style.display = 'block';
  }
}

function OpenPBEMM(sURL) {
  var winPBEMM;
  winPBEMM = window.open(sURL,winPBEMM,"width=700,height=600,resize=yes,scrollbars=yes");
  
}

function OpenKbase() {
  var winKbase;
  winKbase = window.open("http://www.intellicontact.com/knowledgebase/",winKbase,"width=760,height=600,resize=yes,scrollbars=yes"); 
}

function OpenForum() {
  var winForum;
  winForum = window.open("http://www.intellicontact.com/forums/",winForum,"width=780,height=600,resize=yes,scrollbars=yes"); 
  }  

function OpenIntroVideo() {  
  var winIntroVideo;
  winIntroVideo = window.open("/www/video/introduction_video.html",winIntroVideo,"width=720,height=620,resize=yes,scrollbars=yes"); 
  }  
  
    
function RedirectBanner() { 
  document.location.href = "/trial"
}
