
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

SELECT    l.orderkey,
          o.orderdate,
          o.shippriority,
          SUM(l.extendedprice*(1-l.discount)) AS revenue
FROM      CUSTOMER c, ORDERS o, LINEITEM l
WHERE     (c.mktsegment = 'BUILDING' )
      AND (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (o.orderdate < 19950315)
      AND (l.shipdate > 19950315)
GROUP BY  l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC,
         o.orderdate
LIMIT 10;

