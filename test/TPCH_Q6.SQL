CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate int,
  commitdate int,
  receiptdate int,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

SELECT   SUM(l.extendedprice*l.discount) AS revenue
FROM     LINEITEM l
WHERE    (l.shipdate >= 19940101)
     AND (l.shipdate < 19950101)
     AND (l.discount > 0.05)
     AND (l.discount < 0.07)
     AND (l.quantity < 24);
