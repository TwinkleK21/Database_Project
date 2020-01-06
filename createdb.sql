CREATE TABLE yelp_business (
 business_id VARCHAR(100),
 name VARCHAR(100),
 city VARCHAR(100),
 state VARCHAR(100),
 star NUMBER,	
 PRIMARY KEY (business_id)
);


commit;


CREATE TABLE yelp_checkins (
business_id VARCHAR(100),
hour NUMBER,
count NUMBER,
day NUMBER,
PRIMARY KEY (business_id, hour, count,day),
FOREIGN KEY (business_id) REFERENCES yelp_business(business_id) ON DELETE CASCADE
);
commit;

CREATE TABLE hours (
 business_id VARCHAR(100),
 day VARCHAR(100),
 open VARCHAR(100),
 close VARCHAR(100),
 PRIMARY KEY (business_id, day),
 FOREIGN KEY (business_id) REFERENCES yelp_business(business_id) ON DELETE CASCADE
 );

commit;

CREATE TABLE yelp_user (
user_id VARCHAR(50),
name VARCHAR(50),
average_stars NUMBER,
PRIMARY KEY (user_id)
);

commit;

CREATE TABLE yelp_review (
review_id VARCHAR(100),
user_id VARCHAR(100),
business_id VARCHAR(100),
review_date VARCHAR(100),
stars NUMBER,
review_text CLOB,
useful_votes INTEGER,
funny_votes INTEGER,
cool_votes INTEGER,
PRIMARY KEY (review_id),
FOREIGN KEY (user_id) REFERENCES yelp_user(user_id),
FOREIGN KEY (business_id) REFERENCES yelp_business(business_id)
);

commit;


CREATE TABLE categories (
business_id VARCHAR(100),
main_category VARCHAR(100),
PRIMARY KEY (business_id, main_category),
FOREIGN KEY (business_id) REFERENCES yelp_business(business_id) ON DELETE CASCADE
);
commit;


CREATE TABLE sub_categories (
business_id VARCHAR(100),
main_category VARCHAR(100),
sub_category VARCHAR(100),
PRIMARY KEY (business_id, main_category, sub_category),
FOREIGN KEY (business_id, main_category) REFERENCES categories(business_id,main_category) ON DELETE CASCADE
);

commit;

CREATE TABLE attributes (
business_id VARCHAR(100),
attribute VARCHAR(100),
PRIMARY KEY (business_id, attribute),
FOREIGN KEY (business_id) REFERENCES yelp_business(business_id) ON DELETE CASCADE
);
commit;
