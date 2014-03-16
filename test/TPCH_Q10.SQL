CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate int,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

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

CREATE TABLE NATION (
  nationkey int,
  name string,
  regionkey int,
  comment string
) FROM FILE 'nation.tbl' USING TPCH('nation');

SELECT    c.custkey, c.name, 
          c.acctbal, n.name, c.address, c.phone, c.comment,
          sum(l.extendedprice * (1 - l.discount)) as revenue 
FROM      CUSTOMER c, ORDERS o, LINEITEM l, NATION n
WHERE     (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (o.orderdate >= 19931001)
      AND (o.orderdate < 19940101)
      AND (l.returnflag = 'R')
      AND (c.nationkey = n.nationkey)
GROUP BY  c.custkey, c.name, c.acctbal, c.phone, n.name, c.address, c.comment;
