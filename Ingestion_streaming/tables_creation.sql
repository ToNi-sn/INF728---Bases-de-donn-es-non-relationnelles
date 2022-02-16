CREATE KEYSPACE GDELT WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
DESCRIBE KEYSPACES;
USE GDELT;

CREATE TABLE nb_articles_events
(
	"globaleventid" int,    
	"day" int,
	"mentionid"   text,               
	"action_geocountrycode" text,               
	"mentiondoctranslationalinfo" text,                    
	PRIMARY KEY (("day","action_geocountrycode","mentiondoctranslationalinfo"),"globaleventid","mentionid")
);


CREATE TABLE countries_events
(
	"globaleventid" int,
	"day" int,
	"month" int,
	"year" int,
	"nummentions" int,
	"action_geocountrycode" text,
	PRIMARY KEY (("action_geocountrycode"),"month","day","globaleventid")
);


CREATE TABLE data_source
(
	"day" int,
	"month" int,
	"sourcecommonname" text,
	"documentidentifier" text,
	"themes" text,
	"persons" text,
	"locations" text,
	"tone" float,
	PRIMARY KEY (("sourcecommonname"),"day","month","documentidentifier")
);


CREATE TABLE relationship
(
	"sourceurl" text,
	"day" int,
	"month" int,
	"averagetone" float,
	"actor1_geocountrycode" text,
	"actor2_geocountrycode" text,
	"themes" text,
	PRIMARY KEY (("actor1_geocountrycode","actor2_geocountrycode"),"month","day","sourceurl")
);
