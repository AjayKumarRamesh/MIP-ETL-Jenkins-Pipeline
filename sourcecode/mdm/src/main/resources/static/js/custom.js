!function(t,e){function i(){setInterval(function(){w&&(w=!1,l())},50),l()}function n(){t.ajax({url:"https://ibmdev.webmaster.ibm.com/api/randomsaying/",dataType:"text",success:function(e){t("#custom-loading-overlay").find(".ibm-h2")[0].innerHTML=e}})}function o(e){t(".custom-report-listing")[e?"removeClass":"addClass"]("custom-display-all-min"),t(".custom-display-record-full").removeClass("custom-display-record-full")}function r(){var e=t(".custom-search-field").find("input").val();""!==e.trim()&&t("#custom-filter-form").prepend('<input type="hidden" name="q" value="'+e+'">'),t("#custom-filter-form").trigger("submit"),t("#custom-loading-overlay").removeClass("ibm-hide"),t("#ibm-content").addClass("custom-blur")}function c(){var t=y.get();void 0!==e.common.util.url.getParam("id")?o(!0):null!==t&&o(t)}function a(){t("#custom-clearsearch").on("click",function(e){e.preventDefault(),t(".custom-search-field").find("input").val(""),window.location.href="/search/"})}function u(){t("#custom-report-listing").on("click",".custom-ind-record-display-control a",function(e){e.preventDefault(),t(this).closest(".custom-report-item").toggleClass("custom-display-record-full")})}function l(){(e.common.util.scrolledintoview(t(".custom-report-item:eq(-8)").find(".custom-report-list-title"))||e.common.util.scrolledintoview(t("#ibm-footer")))&&h()}function s(){t(".custom-displayicons").on("click","a",function(e){e.preventDefault();var i=t(this).data("full");y.set(i),o(i)})}function m(){t("#custom-filter-form").find("input").on("change",r)}function d(){t(".custom-filter-tags").find("a").on("click",function(e){e.preventDefault();var i=t(this).addClass("ibm-fadeout").data("id");t("#custom-filter-form").find("input[value='"+i+"']").prop("checked",!1),t("#custom-filter-form").find("input:eq(0)").trigger("change")})}function f(){t(".ibm-text-tabs").on("click","a",function(e){e.preventDefault();var i=t(this).data("id"),n=t(".ibm-text-tabs").find("[aria-selected='true']").data("id");t("#custom-filter-form").find("input[value='"+n+"']").prop("checked",!1),"all"!==i?t("#custom-filter-form").find("input[value='"+i+"']").prop("checked",!0).trigger("change"):t("#custom-filter-form").find("input:eq(0)").trigger("change")})}function p(){t("#custom-filter-form").find("[data-widget='scrollable']").each(function(){t(this).height()>245&&t(this).scrollable()})}function g(){var e=t(".custom-search-field");e.find("form").on("submit",function(){t("#custom-loading-overlay").removeClass("ibm-hide")}),e.find(".ibm-search-link").on("click",function(t){t.preventDefault(),e.find("form").trigger("submit")})}function h(){if(!(I&&I<k*C)){k+=1;var i=window.location.href;"?"===i.substring(i.length-1)&&(i=i.substring(0,i.length-1));var n=e.common.util.url.addParam({url:i.replace("/search/","/api/search/"),paramName:"page",paramValue:k});t(document.getElementById("custom-report-listing")).append('<div id="p'+k+'"></div>'),t.ajax({url:n+"&limit="+C+"&datatype=resultshtml",dataType:"json",currentPage:k,success:function(e){e.data.html&&(I=e.data.totalResults,document.getElementById("p"+this.currentPage).innerHTML=e.data.html,v(t(document.getElementById("p"+this.currentPage))),b(t(document.getElementById("p"+this.currentPage)).find(".custom-report-item")))}})}}function v(t){t.find("[data-widget='tooltip']").tooltip({width:410})}function b(e){e.hoverIntent(function(){t(this).find(".custom-editlink").removeClass("ibm-fadeout")},function(){t(this).find(".custom-editlink").addClass("ibm-fadeout")})}var y={itemName:"uxrdisplaylistfull",ttl:31536e3,get:function(){return e.common.util.storage.getItem(this.itemName)},set:function(t){e.common.util.storage.setItem(this.itemName,t,this.ttl)}},w=!1,k=1,C=15,I=null;t(window).scroll(function(){w=!0}),e.common.util.queue.waitForElement("custom-report-listing",c),t(function(){p(),m(),d(),f(),g(),a(),t(".custom-report-item").length>C-1&&i(),s(),u(),v(t(document.getElementById("custom-report-listing"))),setTimeout(n,1e3),b(t(".custom-report-item"))})}(jQuery,IBMCore);