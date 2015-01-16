var _ = require('lodash');
var util = require('util');
var child = require('child_process');
var template = require('fs').readFileSync(__dirname+'/email_template.html', {encoding: 'utf8'});
var data = require(__dirname+'/data');

/*
Email function
*/
var email = function email(to, subject, txt){
	var proc = child.exec(
		util.format('mutt -e "set content_type=text/html" %s -s "%s" ', to, subject)
	);
	proc.stdin.write(txt)
	proc.stdin.end();
};

/*
Insert specific recommendations into template
*/
var format_email = function format_email(user_name, recommend1, recommend2){
	return util.format( template, user_name, recommend1.redirect_url, recommend1.post_name, recommend1.discussion_url, recommend1.post_tagline, recommend2.redirect_url, recommend2.post_name, recommend2.discussion_url, recommend2.post_tagline);
};

var mail_recommendations = function mail_recommendations (user_id, user_name, user_email) {
	data.get_recommendation1(user_id, function(recommend1){;
		data.set_recommended(user_id, recommend1.post_id, function(){;
			data.get_recommendation1(user_id, function(recommend2){;
				data.set_recommended(user_id, recommend2.post_id, function(){;
					var email_body = format_email(user_name, recommend1, recommend2);
					email(user_email, "Product Hunt Recommendations", email_body);
				});
			});
		});
	});
};

var mail_list = function mail_list () {
	email('theodore.popp3@gmail.com', 'Start', 'Product Tribe emails are starting.');
	data.for_subscriber(mail_recommendations);
	email('theodore.popp3@gmail.com', 'End', 'Product Tribe emails are finished.');
}

exports.email = email;
exports.format_email = format_email;
exports.mail_recommendations = mail_recommendations;
exports.mail_list = mail_list;
