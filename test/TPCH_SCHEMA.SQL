CREATE TABLE PART (
  partkey int,
  name string,
  mfgr string,
  brand string,
  type string,
  size int,
  container string,
  retailprice float,
  comment string
) FROM FILE 'part.tbl' USING TPCH('part');

CREATE TABLE SUPPLIER (
  suppkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  comment string
) FROM FILE 'supplier.tbl' USING TPCH('supplier');

CREATE TABLE PARTSUPP (
  partkey int,
  suppkey int,
  availqty int,
  supplycost float,
  comment int
) FROM FILE 'partsupp.tbl' USING TPCH('partsupp');

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

CREATE TABLE REGION (
  regionkey int,
  name string,
  comment string
) FROM FILE 'region.tbl' USING TPCH('region');
