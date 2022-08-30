jQuery(function($) {
           $("#score_div").score();
           $("#feedback_button, #feedback_li").click(function(){
               IBMCore.common.widget.overlay.show('mdm_feedback');
               initFeedback();
               return false;
           });

           function initFeedback(){
              $("#feedback_error_p").hide();
              $("#feedback_error").html("");
              $('#score_div').score('cancel');
              $("#feedback_detail").val("");
              $('#feedback_select').val("").trigger('change');
           }

           $("#feedback_submit").click(function(){
              var errorMessage="";
              var score=$('#score_div').score('score');
              if(score==undefined){
                 errorMessage="please select the score"
              }
              var v=$('#feedback_select option:selected').val();
              if(v==''){
                errorMessage=errorMessage+"<br>please select the feedback type";
              }
              var details=$("#feedback_detail").val();
              if(details==''){
                errorMessage=errorMessage+"<br>please input the feedback details"
              }
              if(errorMessage!=''){
                 $("#feedback_error").html(errorMessage);
                 $("#feedback_error_p").show();
              }else{
                 $.ajax({
                type:'post',
				url: '/mdm/feedback',
				data: {'score':score,'feedbackType':v,'feedbackDetail':details},
				cache: true,
				dataType:'json'
			  }).done(function (data) {
			    if(data==1){
                    alert("feedback success");
				}else if(data==0){
					window.location.href="/mdm/w3Login";
				}else{
				    alert("feedback failed");
				}
				IBMCore.common.widget.overlay.hide('mdm_feedback');
			}).fail(function(error) {
					console.log(error);
	         });
	            }
           });


           $("#feedback_select").change(function(){
               var p=$(this).children('option:selected').val();
               if(p=='suggestion'){
                   $("#feedback_detail_label").html("What is your suggestion?<span class='ibm-required'>*</span>");
               }else if(p=='compliment'){
                    $("#feedback_detail_label").html("Great! What do you like about it?<span class='ibm-required'>*</span>");
               }else if(p=='bug'){
                    $("#feedback_detail_label").html("Sorry you found a bug. Tell us about it, so we can fix it.<span class='ibm-required'>*</span>");
               }else if(p=='question'){
                    $("#feedback_detail_label").html("What is your question?<span class='ibm-required'>*</span>");
               }else{
                    $("#feedback_detail_label").html("");
               }
           });
     });