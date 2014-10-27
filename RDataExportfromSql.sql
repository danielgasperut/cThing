\c
copy (SELECT s.stop_id, 
       s.on_street, 
       s.cross_street, 
       s.on_street || ' + ' || s.cross_street, 
       ss.boardings, 
       ss.alightings, 
       ss.month_begining 
FROM   lookup_route_stop lk 
       JOIN stop s 
         ON ( s.id = lk.stop_id ) 
       JOIN stop_snapshot ss 
         ON ( ss.stop_fk_id = s.id )  )
to '/Users/danielgasperut/Documents/code/civis/BusSnapPG.csv'
with DELIMITER ','
CSV;

copy (SELECT s.stop_id, 
       s.on_street, 
       s.cross_street, 
       s.on_street || ' + ' || s.cross_street, 
       ss.boardings, 
       ss.alightings, 
       ss.month_begining,
       r.route_id,
       r.route_length
FROM   lookup_route_stop lk 
       JOIN stop s 
         ON ( s.id = lk.stop_id ) 
       JOIN stop_snapshot ss 
         ON ( ss.stop_fk_id = s.id )  
       JOIN route r 
       	 ON (r.id = lk.route_id))
to '/Users/danielgasperut/Documents/code/civis/BusRoutePG.csv'
with DELIMITER ','
CSV;
