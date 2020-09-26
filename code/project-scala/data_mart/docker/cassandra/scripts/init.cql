CREATE KEYSPACE labdata WITH REPLICATION = {
      'class' : 'SimpleStrategy', 'replication_factor' : 1
};

CREATE TABLE labdata.clients(

  uid TEXT,
  gender TEXT,
  age INT,
  PRIMARY KEY (uid)

);

INSERT INTO labdata.clients(uid, gender, age) VALUES ('test1', 'M', 10);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test2', 'F', 20);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test3', 'M', 30);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test4', 'F', 40);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test5', 'M', 50);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test6', 'F', 60);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test7', 'M', 53);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test8', 'F', 42);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test9', 'M', 33);
INSERT INTO labdata.clients(uid, gender, age) VALUES ('test10', 'F', 24);