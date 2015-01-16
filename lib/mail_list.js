var mail = require(__dirname+'/mail');

mail.mail_list();
setTimeout(function(){
	process.exit();
}, 1000*60*60*24);
