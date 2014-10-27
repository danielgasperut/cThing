\c civis

--Cleanup
/*
Drop table BusData_stage;
Drop table lk_route_stop;
Drop table d_Route;
Drop table d_Stop;
Drop Table f_stop_snapshot;
*/

--Create DB Structure. 
--Landing
CREATE TABLE if not exists BusData_stage  (
	stop_id integer,
	on_street varchar(250),
	cross_street varchar(250),
	routes varchar(400),
	boardings numeric(8,2),
	alightings numeric(8,2),
	month_beginning date,
	daytype varchar(40),
	location varchar(250)
	);

--Staging
CREATE TABLE if not exists lk_route_stop  (
	route_id varchar(6),
 	stop_id integer
 	);

--Pub
CREATE TABLE if not exists d_Route  (
	route_pk integer, --since route ID is a string, lets give it a pk
	route_id varchar(6),
	stop_list varchar(4000),
	route_length integer,
	google_maps_route varchar(4000)
);

CREATE TABLE if not exists d_Stop (
	stop_id integer,
	routes varchar(400), --not 3nf :(
	on_street varchar(250),
	cross_street varchar(250),
	location varchar(250)
);

Create Table if not exists f_stop_snapshot (
	stop_id integer,
	boardings numeric(8,2),
	alightings numeric(8,2),
	month_beginning date
);

--little audit table, this would be easier to log if these were stored procedures, you'd have a variable
Create Table if not exists ABC (
	load_id integer,
	load_date timestamp,
	lnd_busdata_stage_bal integer,
	lnd_lk_route_stop integer,
	pub_d_stop_bal integer,
	pub_d_route_bal integer,
	pub_f_stop_snapshot_bal integer
);

--Load
--land bus data stage
Truncate table BusData_stage;

\set inPath :inFolder'BusData.csv'

Copy busdata_stage (stop_id,on_street,cross_street,routes,boardings,alightings,month_beginning,daytype,location)
FROM :'inPath'
WITH DELIMITER ','
CSV HEADER;
--INSERT 0 11593  

--some transformation (lets make route more useful as an attribute)
Truncate table lk_route_stop;

insert into lk_route_stop (route_id, stop_id) (
select distinct  
trim(regexp_split_to_table(routes,E',')), 
stop_id
from busdata_stage); 
--INSERT 0 15270

--load publish tables. Users can more easily run analysis on these arranged tables

INSERT INTO d_Stop (stop_id, routes, on_street, cross_street, LOCATION)
    (SELECT s.stop_id,
            s.routes,
            s.on_street,
            s.cross_street,
            s.LOCATION
     FROM BusData_stage s
     WHERE NOT EXISTS
             (SELECT NULL
              FROM d_stop d
              WHERE d.stop_id = s.stop_id));

--INSERT 0 11593

INSERT INTO f_stop_snapshot(stop_id, boardings, alightings, month_beginning)
    (SELECT s.stop_id,
            s.boardings,
            s.alightings,
            s.month_beginning
     FROM BusData_stage s
     WHERE NOT EXISTS
             (SELECT NULL
              FROM f_stop_snapshot f
              WHERE f.stop_id = s.stop_id
                  AND f.month_beginning = s.month_beginning));

 --route dim, ordered, lets rebuild each time

Truncate table d_route;

INSERT INTO d_route (route_pk, route_id, stop_list, route_length, google_maps_route)
SELECT
    (SELECT coalesce(max(route_pk)+1, 1) FROM d_route) route_pk,
           s.route_id,
           array_to_string(array_agg(s.stop_id
                                     ORDER BY s.stop_id), ',') ,
           count(s.stop_id),
           ''
	FROM lk_route_stop s
	WHERE NOT EXISTS
        (SELECT NULL
         FROM d_route d
         WHERE s.route_id = d.route_id)
GROUP BY route_id;



--audit

insert into ABC 
(
	select
	(select coalesce(max(load_id) + 1, 1) from ABC),
	current_timestamp,
	(select count(*) from busdata_stage),
	(select count(*) from lk_route_stop),
	(select count(*) from d_stop),
	(select count(*) from d_route),
	(select count(*) from f_stop_snapshot)

);
		
