create table friends.seasons
(
  season_id int,
  season_name varchar(10),
  PRIMARY KEY(season_id)
)

create table friends.episodes
(
	season_id int,
	episode_id int,
	episode_name varchar(500),
	url varchar(500),
	PRIMARY KEY(season_id,episode_id),
	FOREIGN KEY(season_id) references friends.seasons(season_id)
)


create table friends.dialogues
(
	season_id int,
	episode_id int,
	dialogue_id int,
	dialogue TEXT,
	characters varchar(500),
	PRIMARY KEY(season_id,episode_id,dialogue_id),
	FOREIGN KEY(season_id,episode_id) references friends.episodes(season_id,episode_id)
)

create table friends.aggregate_season
(
	season_id int,
	characters varchar(500),
	metric varchar(20),
	value double,
	PRIMARY KEY(season_id,characters,metric),
	FOREIGN KEY(season_id) references friends.seasons(season_id)
)

create table friends.aggregate_overall
(
	characters varchar(500),
	metric varchar(20),
	value int,
	PRIMARY KEY(characters)
)

