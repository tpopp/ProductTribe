var data = require(__dirname + '/data');

data.update_posts();
data.for_subscriber(data.update_user);
setTimeout(function(){
	process.exit();
}, 1000*60*60*24);
