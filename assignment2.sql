CREATE TABLE query1 AS
SELECT name, COUNT(movieid) AS moviecount
FROM genres G, hasagenre H
WHERE G.genreid=H.genreid
GROUP BY name;

CREATE TABLE query2 AS
SELECT name, AVG(rating) AS rating
FROM genres G, ratings R, hasagenre H
WHERE G.genreid= H.genreid AND H.movieid=R.movieid
GROUP BY G.genreid;

CREATE TABLE query3 AS
SELECT title, COUNT(rating) AS countofratings
FROM movies M, ratings R
WHERE M.movieid=R.movieid
GROUP BY M.movieid
HAVING COUNT(rating)>=10;

CREATE TABLE query4 AS
SELECT M.movieid as movieid,title
FROM movies M, genres G, hasagenre H
WHERE M.movieid=H.movieid AND G.genreid=H.genreid AND G.name ='Comedy';

CREATE TABLE query5 AS
SELECT title, AVG(rating) AS average
FROM movies M, ratings R
WHERE M.movieid=R.movieid
GROUP BY M.movieid;

CREATE TABLE query6 AS
SELECT AVG(rating) AS average
FROM genres G, ratings R, hasagenre H
WHERE H.movieid=R.movieid AND G.genreid=H.genreid AND G.name='Comedy';

CREATE TABLE query7 AS
SELECT AVG(rating) AS average
FROM genres G1, genres G2, ratings R, hasagenre H1, hasagenre H2
WHERE G1.genreid=H1.genreid AND G2.genreid=H2.genreid AND H1.movieid=R.movieid AND H2.movieid=R.movieid AND G1.name ='Comedy' AND G2.name ='Romance';

CREATE TABLE query8 AS
SELECT AVG(rating) AS average
FROM genres G,ratings R, hasagenre H
WHERE G.genreid=H.genreid  AND H.movieid=R.movieid AND G.name ='Romance' AND H.movieid NOT IN (
SELECT movieid
FROM genres G, hasagenre H
WHERE G.genreid=H.genreid AND G.name='Comedy');

CREATE TABLE query9 AS
SELECT movieid, rating
FROM ratings R
WHERE R.userid = :v1;

CREATE VIEW view1 AS
SELECT movieid, rating
FROM ratings R
WHERE R.userid = :v1;

CREATE VIEW view2 AS
SELECT movieid, rating
FROM ratings R
WHERE R.movieid NOT IN (SELECT movieid FROM ratings R WHERE R.userid= :v1);

CREATE VIEW avgil AS
SELECT M.movieid AS movieid, AVG(rating) AS average
FROM movies M, ratings R
WHERE M.movieid=R.movieid
GROUP BY M.movieid;

CREATE VIEW similarity AS
SELECT I.movieid as movieid1, L.movieid AS movieid2, 1-(abs(AI.average - AL.average))/5 AS sim
FROM view1 L, view2 I, avgil AI, avgil AL
WHERE I.movieid=AI.movieid AND L.movieid=AL.movieid;
 
CREATE VIEW prediction AS
SELECT S.movieid1 as movieid1, SUM(S.sim*L.rating)/SUM(S.sim) AS prediction
FROM similarity S, view1 L
WHERE S.movieid2=L.movieid
GROUP BY S.movieid1;

CREATE TABLE recommendation AS
SELECT DISTINCT M.title AS title
FROM movies M, prediction P
WHERE M.movieid = P.movieid1 AND P.prediction > 3.9;



