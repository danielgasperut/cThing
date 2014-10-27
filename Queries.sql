
--longest route
civis=# select count(*), route_id from lk_route_stop group by route_id order by count(*) desc limit 1;
count | route_id
-------+----------
   273 | 9
(1 row)

or civis=# select route_length, route_id from d_route order by route_length desc limit 1;
 route_length | route_id
--------------+----------
          273 | 9


--find stop on most routes
select count(*), stop_id from lk_route_stop group by stop_id order by count(*) desc  limit 1;
 count | stop_id
-------+---------
    14 |    1106