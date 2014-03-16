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

SELECT    SUM(l.extendedprice * (1 - l.discount) ) AS revenue
FROM      LINEITEM l, PART p 
WHERE  (
            (p.partkey = l.partkey)
        AND (p.brand = 'Brand#12')
        AND (    (p.container = 'SM CASE')
              OR (p.container = 'SM BOX')
              OR (p.container = 'SM PACK')
              OR (p.container = 'SM PKG')
            )
        AND (l.quantity >= 1)
        AND (l.quantity <= (1 + 10))
        AND (p.size >= 1)
        AND (p.size <= 5)
        AND (    (l.shipmode = 'AIR')
              OR (l.shipmode = 'AIR REG')
            )
        AND (l.shipinstruct = 'DELIVER IN PERSON')
      ) OR (
            (p.partkey = l.partkey)
        AND (p.brand = 'Brand#23')
        AND (    (p.container = 'MED BAG')
              OR (p.container = 'MED BOX')
              OR (p.container = 'MED PKG')
              OR (p.container = 'MED PACK')
            )
        AND (l.quantity >= 10)
        AND (l.quantity <= (10 + 10))
        AND (p.size >= 1)
        AND (p.size <= 10)
        AND (    (l.shipmode = 'AIR')
              OR (l.shipmode = 'AIR REG')
            )
        AND (l.shipinstruct = 'DELIVER IN PERSON')
      ) OR (
            (p.partkey = l.partkey)
        AND (p.brand = 'Brand#34')
        AND (    (p.container = 'LG CASE')
              OR (p.container = 'LG BOX')
              OR (p.container = 'LG PACK')
              OR (p.container = 'LG PKG')
            )
        AND (l.quantity >= 20)
        AND (l.quantity <= (20 + 10))
        AND (p.size >= 1)
        AND (p.size <= 15)
        AND (    (l.shipmode = 'AIR')
              OR (l.shipmode = 'AIR REG')
            )
        AND (l.shipinstruct = 'DELIVER IN PERSON')
      );
