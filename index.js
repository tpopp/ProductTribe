var util = require('util');
var express = require('express');
var app = express();
var passport = require('passport');
var config = require('./lib/config');
var data = require('./lib/data');
var mail = require('./lib/mail');
var bodyParser = require('body-parser');
var schedule = require('node-schedule');
var child = require('child_process');
var logger = require('express-logger');

passport.use('producthunt', config.OAuthStrategy);
passport.serializeUser(function(user, done) {
  done(null, user);
});
passport.deserializeUser(function(user, done) {
  done(null, user);
});

app.use(require('compression')());
app.use(logger({path: __dirname + "/express.log"}));
app.use(passport.initialize());
app.use(passport.session());
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.use( bodyParser.json() );       // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}));
app.use('/css', express.static(__dirname + '/public/css'));
app.use('/img', express.static(__dirname + '/public/img'));
app.use('/js', express.static(__dirname + '/public/js'));


app.get('/', function (req, res) {
        res.render('index');
});

app.get('/auth/producthunt', passport.authenticate('producthunt', {scope: ['public', 'private']}));

app.get('/auth/producthunt/callback', passport.authenticate('producthunt'), function (req, res){
	var user = req.user.user;
        if(user.name && user.email){
		data.subscribe_user(user.id, user.name, user.email);
                res.render('subscribed', user);
		mail.mail_recommendations(user.id, user.name, user.email);
	} else if(user.name){
		res.render('subscribe_email');
	} else {
                res.render('failed');
	}
});

app.post('/auth/producthunt/callback', function(req, res){
		data.subscribe_user(req.body.id, req.body.name, req.body.email);
		var user = {};
		user.name = req.body.name;
		user.email = req.body.email;
		user.image_url = {};
		user.image_url["88px"] = req.body.pic;
                res.render('subscribed', user);
		mail.mail_recommendations(req.body.id, req.body.name, req.body.email);
});

app.get('/unsubscribe', function(req, res){
        res.render('unsubscribe');
});
app.post('/unsubscribe', function(req, res){
	data.unsubscribe_user(req.body.email);
        res.redirect('/');
});

var server = app.listen(3001);

var rule = new schedule.RecurrenceRule();
rule.hour = 2;
rule.minute = 0;
var mailing = schedule.scheduleJob(rule,function(){
        child.fork(__dirname+'/lib/mail_list.js');
});


var rule1 = new schedule.RecurrenceRule();
rule1.hour = 23;
rule1.minute = 0;
var updating = schedule.scheduleJob(rule1,function(){
        child.fork(__dirname+'/lib/update_data.js');
});
