-- The following query doesn't execute in Snowflake. Placed here for reference:
-- *****************************************************************************
-- SQL compilation error: syntax error line 8 at position 21 unexpected 'from'.
-- syntax error line 8 at position 26 unexpected '1'. syntax error line 10 at position 2 unexpected
-- 'from'. syntax error line 13 at position 21 unexpected 'from'. syntax error line 16 at position 4
-- unexpected 'select'. syntax error line 17 at position 9 unexpected 'c_acctbal'. syntax error line 22
-- at position 27 unexpected 'from'. syntax error line 24 at position 3 unexpected ')'.
-- *****************************************************************************
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            customer
        where
                substring(c_phone from 1 for 2) in
                ('30', '24', '31', '38', '25', '34', '37')
          and c_acctbal > (
            select
                avg(c_acctbal)
            from
                customer
            where
                    c_acctbal > 0.00::fixeddecimal
              and substring(c_phone from 1 for 2) in
                  ('30', '24', '31', '38', '25', '34', '37')
        )
          and not exists (
                select
                    *
                from
                    orders
                where
                        o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode