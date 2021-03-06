CREATE TABLE HELLOWORLD (
   HELLO VARCHAR(15),
   WORLD VARCHAR(15),
   DIALECT VARCHAR(15) NOT NULL,
   PRIMARY KEY (DIALECT)
);

CREATE TABLE USERACCOUNT (
   EMAIL VARCHAR(128) UNIQUE NOT NULL,
   FIRSTNAME VARCHAR(15),
   LASTNAME VARCHAR(15),
   LASTLOGIN TIMESTAMP,
   DIALECT VARCHAR(15) NOT NULL,
   PRIMARY KEY (EMAIL)
);

PARTITION TABLE USERACCOUNT ON COLUMN EMAIL;

LOAD CLASSES helloworld.jar;

CREATE PROCEDURE Insert
   AS INSERT INTO HELLOWORLD (Dialect, Hello, World) VALUES (?, ?, ?);

CREATE PROCEDURE RegisterUser
    PARTITION ON TABLE Useraccount COLUMN Email
    AS INSERT INTO USERACCOUNT
      (Email, Firstname, Lastname, Dialect)
      VALUES (?,?,?,?);

CREATE PROCEDURE
    PARTITION ON TABLE Useraccount COLUMN Email
    FROM CLASS SignIn;
