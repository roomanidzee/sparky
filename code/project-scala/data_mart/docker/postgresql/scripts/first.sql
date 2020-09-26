
CREATE DATABASE labdata;
CREATE DATABASE andrey_romanov;

CREATE USER andrey_romanov WITH PASSWORD 'test_pass';

CREATE TABLE domain_cats(

  domain VARCHAR(100) PRIMARY KEY,
  category VARCHAR(100)

);