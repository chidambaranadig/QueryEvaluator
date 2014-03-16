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

SELECT   l.returnflag, l.linestatus,
         SUM(l.quantity) AS sum_qty,
         SUM(l.extendedprice) AS sum_base_price, 
         SUM(l.extendedprice*(1-l.discount)) AS sum_disc_price, 
         SUM(l.extendedprice*(1-l.discount)*(1+l.tax)) AS sum_charge, 
         AVG(l.quantity) AS avg_qty,
         AVG(l.extendedprice) AS avg_price,
         AVG(l.discount) AS avg_disc,
         COUNT(*) AS count_order
FROM     LINEITEM l
WHERE    l.shipdate <= 19981101
GROUP BY l.returnflag, l.linestatus;
