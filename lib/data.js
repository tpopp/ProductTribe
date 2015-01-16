var pg = require('pg');
var copyFrom = require('pg-copy-streams').from;
var fs = require('fs');
var config = require('./config');
var _ = require('lodash')
var https = require('https');
var util = require('util');

/*
var a = {
	path: '/v1/posts/all',
	older: '22'
};

var req = https.request(config.get_request(a), function(res){
	var body = '';
	res.on('data', function (data){
		body += data;
	});
	res.on('end',  function(){
		console.log(JSON.parse(body));
	});
}).end();
*/

var update_posts = function update_posts (ind) {
	var start = 0;
	if (ind){
		start = ind;
		console.log(start);
		var poll = {
			path: '/v1/posts/all',
			newer: start
		};

		var req = https.request(config.get_request(poll), function(res){
			var body = '';
			res.on('data', function (data){
				body += data;
			});
			res.on('end',  function(){
				obj = JSON.parse(body);
				if(obj.posts.length === 0) {
					console.log('done updating posts');
					return;
				}
				pg.connect(config.database, function(err, client, done){
					if(handleError(err, client, done)) return false;
					for(var i = 0; i < obj.posts.length; i++){
						var post = obj.posts[i];
						start = start > post.id ? start : post.id;
						client.query('INSERT INTO posts SELECT $1, $2, $3, $4, $5, $6 WHERE NOT EXISTS (SELECT post_id FROM posts WHERE $1 = post_id)',
									[post.id, post.name, post.tagline, post.votes_count, post.redirect_url, post.discussion_url], function(err, result){
							if(handleError(err, client, done)) return false;
						});
					}
					client.on('drain', function(){
						done();
						setTimeout(update_posts, 3000, start);
					});
				});
			});
		}).end();
			

	} else {
		pg.connect(config.database, function(err, client, done){
			if(handleError(err, client, done)) return false;
			client.query('SELECT MAX(post_id) as post FROM posts',
						[], function(err, result){
				if(handleError(err, client, done)) return false;
				start = result.rows[0].post;
				done();
				update_posts(start);
			});
		});
	}
};

var update_votes = function update_votes () {
	if (ind){
		var poll = {
			path: '/v1/posts/all',
			newer: start
		};

		var req = https.request(config.get_request(a), function(res){
			var body = '';
			res.on('data', function (data){
				body += data;
			});
			res.on('end',  function(){
				obj = JSON.parse(body);
				if(obj.posts.length === 0) return;
					pg.connect(config.database, function(err, client, done){
						if(handleError(err, client, done)) return false;
						for(var i = 0; i < obj.posts.length; i++){
							client.query('INSERT INTO posts VALUES ($!, $2. $3, $4, $5, $6)',
										[], function(err, result){
								if(handleError(err, client, done)) return false;
							});
						}
						client.on('drain', client.end.bind(client));
				});
			});
		}).end();
			

	} else {
		var start = 0;
		pg.connect(config.database, function(err, client, done){
			if(handleError(err, client, done)) return false;
			client.query('SELECT MAX(vote_id) as id FROM votes',
						[], function(err, result){
				if(handleError(err, client, done)) return false;
				start = result.rows[0].id;
				done();
				update_posts(start);
			});
		});
	}

};

var update_user = function update_user (user_id, ind) {
	if (typeof ind == 'number'){
		var start = ind;
		var poll = {
			path: util.format('/v1/users/%d/votes', user_id),
			newer: start
		};

		var req = https.request(config.get_request(poll), function(res){
			var body = '';
			res.on('data', function (data){
				body += data;
			});
			res.on('end',  function(){
				obj = JSON.parse(body);
				if(obj.votes.length === 0) {
					console.log('done updating user');
					return;
				}
				pg.connect(config.database, function(err, client, done){
					if(handleError(err, client, done)) return false;
					for(var i = 0; i < obj.votes.length; i++){
						var vote = obj.votes[i];
						start = start > vote.id ? start : vote.id;
						client.query('INSERT INTO votes SELECT $1, $2, $3 WHERE NOT EXISTS (SELECT * FROM votes WHERE $1 = vote_id AND $2 = user_id AND $3 = post_id)',
									[vote.id, user_id, vote.post_id], function(err, result){
							if(handleError(err, client, done)) return false;
						});
					}
					client.on('drain', function(){
						done();
						setTimeout(update_user, 3000, user_id, start);
					});
				});
			});
		}).end();
	} else {
		pg.connect(config.database, function(err, client, done){
			if(handleError(err, client, done)) return false;
			client.query('SELECT MAX(vote_id) as id FROM votes WHERE user_id = $1',
						[user_id], function(err, result){
				if(handleError(err, client, done)) return false;
				if(result.rows[0].id === null){
					done();
					return update_user(user_id, 0);
				}
				start = result.rows[0].id;
				done();
				update_user(user_id, start);
			});
		});
	}


};


/*
If an error occured, remove the connection
*/
var handleError = function handleError(err, client, done) {
	if(!err) {
		return false;
	}

	console.log(err);

	done(client);
	return true;
};

/*
pg.connect(config.database, function(err, client, done){
	var query = client.query('SELECT * FROM bla', [], function(err, result){
		if(handleError(err, client, done)) return false;
		console.log(result);
		done();
	});

	query.on('row', function(row){
		console.log(row);
	});

});
*/

/*
Create the necessary tables
*/
var create_tables = function create_tables(){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('CREATE TABLE IF NOT EXISTS votes(vote_id integer NOT NULL, user_id integer NOT NULL, post_id integer NOT NULL, UNIQUE(user_id, post_id))',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
		});

		client.query('CREATE TABLE IF NOT EXISTS posts(post_id integer PRIMARY KEY, post_name varchar(50) NOT NULL, post_tagline varchar(100) NOT NULL, post_votes integer NOT NULL, ' +
				'redirect_url varchar(100), discussion_url varchar(100))',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
		});

		client.query('CREATE TABLE IF NOT EXISTS subscribers(user_id integer PRIMARY KEY, user_name varchar(50) NOT NULL, user_email varchar(50) NOT NULL)',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
		});

		client.query('CREATE TABLE IF NOT EXISTS connected_posts(original_post_id integer NOT NULL, connected_post_id integer NOT NULL, connectedness_rating integer NOT NULL, ' +
				'UNIQUE(original_post_id, connected_post_id))',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
		});

		client.query('CREATE TABLE IF NOT EXISTS recommendations_1(user_id integer NOT NULL, post_id integer NOT NULL, strength integer NOT NULL, UNIQUE(user_id, post_id))',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
		});

		client.query('CREATE TABLE IF NOT EXISTS recommendations_2(user_id integer NOT NULL, post_id integer NOT NULL, strength integer NOT NULL, UNIQUE(user_id, post_id))',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
		});

		client.query('CREATE TABLE IF NOT EXISTS recommended(user_id integer NOT NULL, post_id integer NOT NULL, UNIQUE(user_id, post_id))',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
			done();
		});

		return true;

	});

};

var calculate_connectedness = function calculate_connectedness (client, done) {
	client.query('INSERT INTO connected_posts SELECT DISTINCT ON (a.post_id, b.post_id) a.post_id, b.post_id, COUNT(*) FROM votes a INNER JOIN votes b USING (user_id) WHERE a.post_id != b.post_id GROUP BY a.post_id, b.post_id', [], function(err, result){
		if(handleError(err, client, done)) return false;
		console.log('done with connectedness');
		done();
	});
};

/*
ingest vote file into database
*/
var ingest_votes = function ingest_votes(votes_file){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('CREATE TEMP TABLE temp(id integer, created_at timestamp, user_id integer, post_id integer, user_username text, post_name text, post_tagline text, post_discussion_url text)',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
			var stream = client.query(copyFrom("COPY temp FROM STDIN WITH DELIMITER ';' CSV HEADER"));
			var fileStream = fs.createReadStream(votes_file);
			fileStream.on('error', done);
			fileStream.pipe(stream).on('finish', function(){
				client.query('INSERT INTO votes SELECT DISTINCT ON (user_id, post_id) id, user_id, post_id FROM temp',
							[], function(err, result){

					if(handleError(err, client, done)) return false;
					console.log('done with votes');
					return calculate_connectedness(client, done);

				});
			}).on('error', done);
		});

	});

};


/*
ingest post file into database
*/
var ingest_posts = function ingest_posts(posts_file){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('CREATE TEMP TABLE temp(id integer, created_at timestamp, name varchar(30), tagline varchar(100), user_id integer, user_username varchar(30), votes_count integer, comments_count integer, redirect_url varchar(100), discussion_url varchar(100))',
					[], function(err, result){
			if(handleError(err, client, done)) return false;
			var stream = client.query(copyFrom("COPY temp FROM STDIN WITH DELIMITER ';' CSV HEADER"));
			var fileStream = fs.createReadStream(posts_file);
			fileStream.on('error', function(a){console.log(a);});
			fileStream.pipe(stream).on('finish', function(){
				client.query('INSERT INTO posts SELECT DISTINCT id, name, tagline, votes_count, redirect_url, discussion_url FROM temp',
							[], function(err, result){

					if(handleError(err, client, done)) return false;
					console.log('done with posts');
					done();

				});
			});
		});
	});

};

/*
Mark post as recommended for user, so it isn't recommended again
*/
var set_recommended = function set_recommended(user, post, call){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('DELETE FROM recommendations_1 WHERE user_id = $1 AND post_id = $2',
					[user, post], function(err, result){
			if(handleError(err, client, done)) return false;
			client.query('DELETE FROM recommendations_2 WHERE user_id = $1 AND post_id = $2',
						[user, post], function(err, result){
				if(handleError(err, client, done)) return false;
				client.query('INSERT INTO recommended VALUES ($1, $2)',
							[user, post], function(err, result){
					if(handleError(err, client, done)) return false;
					done();
					call();
				});
			});
		});
	});

};

var get_post = function get_post(post_id, call){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('SELECT * FROM posts WHERE post_id = $1', [post_id], function(err, result){
			if(handleError(err, client, done)) return false;
			done();
			call(result.rows[0]);
		});
	});
};

/*
Genereate more recommendations for the given person
*/
var generate_recommendation1 = function generate_recommendation1 (user_id, call){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('INSERT INTO recommendations_1 (user_id, post_id, strength) SELECT a.user_id, b.connected_post_id, SUM (b.connectedness_rating) AS rating FROM votes a INNER JOIN connected_posts b ON a.post_id = b.original_post_id AND $1 = a.user_id WHERE NOT EXISTS ( SELECT user_id, post_id FROM votes WHERE a.user_id = user_id AND b.connected_post_id = post_id) AND NOT EXISTS ( SELECT user_id, post_id FROM recommended WHERE a.user_id = user_id AND b.connected_post_id = post_id ) AND NOT EXISTS ( SELECT user_id, post_id FROM recommendations_1 WHERE a.user_id = user_id AND b.connected_post_id = post_id ) GROUP BY a.user_id, b.connected_post_id ORDER BY rating DESC LIMIT 20',
					[user_id], function(err, result){
			if(handleError(err, client, done)) return false;
			client.query('SELECT * FROM recommendations_1 WHERE user_id = $1', [user_id], function(result){
				if(handleError(err, client, done)) return false;
				if(! result ){
					client.query('INSERT INTO recommendations_1(user_id, post_id, strength) SELECT $1, post_id, post_votes FROM posts WHERE NOT EXISTS ( SELECT user_id, post_id FROM votes WHERE user_id = $1 AND votes.post_id = posts.post_id) AND NOT EXISTS ( SELECT user_id, post_id FROM recommended WHERE $1 = user_id AND recommended.post_id = posts.post_id ) AND NOT EXISTS ( SELECT user_id, post_id FROM recommendations_1 WHERE $1 = user_id AND recommendations_1.post_id = post_id ) ORDER BY post_votes DESC LIMIT 20', [user_id], function(){
						if(handleError(err, client, done)) return false;
						call(user_id);
					});
				} else {
					done();
					call(user_id);
				}
			});
		});

	});

};

/*
Genereate more recommendations for the given person
*/
var generate_recommendation2 = function generate_recommendation2 (user_id){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('INSERT INTO recommendations_2 (user_id, post_id, strength) SELECT user_id, post_id, strength FROM ',
					[user_id], function(err, result){
			if(handleError(err, client, done)) return false;
			done();
		});

	});

};

/*
Get recommendation from method one for the given user
*/
var get_recommendation1 = function get_recommendation1(user_id, call){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('SELECT * FROM recommendations_1 WHERE user_id = $1 ORDER BY strength DESC LIMIT 1',
					[user_id], function(err, result){
			if(handleError(err, client, done)) return false;
			if(result.rows.length){
				done();
				get_post(result.rows[0].post_id, call);
			} else {
				done();
				generate_recommendation1(user_id, _.partialRight(get_recommendation1, call));
			}
		});

	});

};

/*
Get recommendation from method two for the given user
*/
var get_recommendation2 = function get_recommendation2(user_id){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('SELECT * FROM recommendations_2 WHERE user_id = $1 ORDER BY strength DESC LIMIT 1',
					[user_id], function(err, result){
			if(handleError(err, client, done)) return false;
			if(result.rows.length){
				done();
				return result.rows[0];
			} else {
				done();
				generate_recommendation2(user_id);
				return get_recommendation2(user_id);
			}
		});

	});

};

/*
Add user to the mailing list
*/
var subscribe_user = function subscribe_user(user_id, user_name, user_email){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;
		client.query('INSERT INTO subscribers(user_id, user_name, user_email) VALUES ($1, $2, $3)',
					[user_id, user_name, user_email], function(err, result){
			if(handleError(err, client, done)) return false;
			done();
		});

	});

};


/*
Remove user from the mailing list
*/
var unsubscribe_user = function unsubscribe_user(email){
	pg.connect(config.database, function(err, client, done){
		if(handleError(err, client, done)) return false;

		client.query('SELECT user_id FROM subscribers WHERE user_email = $1',
					[email], function(err, result){
			if(handleError(err, client, done)) return false;
			if(! result.rows.length){
				done();
				return true;
			}
			var id = result.rows[0].user_id;

			client.query('DELETE FROM recommendations_1 WHERE user_id = $1',
						[id], function(err, result){
				if(handleError(err, client, done)) return false;
			});

			client.query('DELETE FROM recommendations_2 WHERE user_id = $1',
						[id], function(err, result){
				if(handleError(err, client, done)) return false;
			});

			client.query('DELETE FROM recommended WHERE user_id = $1',
						[id], function(err, result){
				if(handleError(err, client, done)) return false;
			});

		});

		client.query('DELETE FROM subscribers WHERE user_email = $1',
					[email], function(err, result){
			if(handleError(err, client, done)) return false;
			done();
		});

	});

};

var for_subscriber = function for_subscriber (call) {
	pg.connect(config.database, function(err, client, done){
		var query = client.query('SELECT * FROM subscribers', [], function(err, result){
			if(handleError(err, client, done)) return false;
			done();
		});

		query.on('row', function(row){
			call(row.user_id, row.user_name, row.user_email);
		});

	});

};

exports.for_subscriber = for_subscriber;
exports.create_tables = create_tables;
exports.ingest_votes = ingest_votes;
exports.ingest_posts = ingest_posts;
exports.generate_recommendation1 = generate_recommendation1;
exports.generate_recommendation2 = generate_recommendation2;
exports.get_recommendation1 = get_recommendation1;
exports.get_recommendation2 = get_recommendation2;
exports.subscribe_user = subscribe_user;
exports.unsubscribe_user = unsubscribe_user;
exports.set_recommended = set_recommended;
exports.get_post = get_post;
exports.update_user = update_user;
exports.update_votes = update_votes;
exports.update_posts = update_posts;
