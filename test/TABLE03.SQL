CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;

SELECT R.A, S.C FROM R JOIN S ON R.B = S.B;
