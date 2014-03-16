CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;
CREATE TABLE T ( C int, D int ) FROM FILE 'test/t.dat' USING CSV;

SELECT R.A, T.D FROM R, S, T WHERE (R.B = S.B) AND (T.C < S.C);