CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/r.dat' USING CSV;

SELECT COUNT(*) FROM R, S;