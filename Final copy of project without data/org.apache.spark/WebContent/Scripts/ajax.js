function getAjaxRequest(){
	var ajaxRequest;  // The variable that makes Ajax possible!
	
	try{
		// Opera 8.0+, Firefox, Safari
		ajaxRequest = new XMLHttpRequest();
	} catch (e){
		// Internet Explorer Browsers
		try{
			ajaxRequest = new ActiveXObject("Msxml2.XMLHTTP");
		} catch (e) {
			try{
				ajaxRequest = new ActiveXObject("Microsoft.XMLHTTP");
			} catch (e){
				// Something went wrong
				alert("Your browser broke!");
				return false;
			}
		}
	}
	
	return ajaxRequest;
}

function trim(str) {
    return str.replace(/^\s*|\s*$|\n|\r/g,"");
}

function buildQueryString(params) {
    var query = "";
    for (var i = 0; i < params.length; i++) {
        query += (i > 0 ? "&" : "")
            + escape(params[i].name) + "="
            + escape(params[i].value);
    }
    return query;
}

function ajaxDownloadClick(item) {
    var ajaxRequest = getAjaxRequest();
    ajaxRequest.onreadystatechange = function(){
      if(ajaxRequest.readyState == 4){
      }
    }

    var params = [
        { name: "item", value: item },
        { name: "type", value: "Download" }
    ];
    var body = buildQueryString(params);

    ajaxRequest.open("POST", "LinkClicked.aspx?rnd=" + Math.random(), true);
    ajaxRequest.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
    ajaxRequest.send(body); 
}

function ajaxLinkClick(item) {
    var ajaxRequest = getAjaxRequest();
    ajaxRequest.onreadystatechange = function(){
      if(ajaxRequest.readyState == 4){
      }
    }

    var params = [
        { name: "item", value: item },
        { name: "type", value: "Buy Link" }
    ];
    var body = buildQueryString(params);

    ajaxRequest.open("POST", "LinkClicked.aspx?rnd=" + Math.random(), true);
    ajaxRequest.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
    ajaxRequest.send(body); 
}
