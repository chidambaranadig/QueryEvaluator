
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

CREATE TABLE SUPPLIER (
  suppkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  comment string
) FROM FILE 'supplier.tbl' USING TPCH('supplier');

CREATE TABLE NATION (
  nationkey int,
  name string,
  regionkey int,
  comment string
) FROM FILE 'nation.tbl' USING TPCH('nation');

CREATE TABLE REGION (
  regionkey int,
  name string,
  comment string
) FROM FILE 'region.tbl' USING TPCH('region');

SELECT    n.name,
          SUM(l.extendedprice * (1 - l.discount)) AS revenue
FROM      CUSTOMER c, ORDERS o, LINEITEM l, SUPPLIER s, NATION n, REGION r
WHERE     (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (l.suppkey = s.suppkey)
      AND (c.nationkey = s.nationkey)
      AND (s.nationkey = n.nationkey)
      AND (n.regionkey = r.regionkey)
      AND (r.name = 'ASIA')
      AND (o.orderdate >= 19940101)
      AND (o.orderdate < 19950101)
GROUP BY  n.name
ORDER BY revenue DESC;
