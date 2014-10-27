#http://data.cityofchicago.org/resource/mq3i-nnqe.json
#from peewee import * #SqliteDatabase, CharField, ForeignKeyField, Model, PostgresqlDatabase
from playhouse.postgres_ext import *
import requests

db = PostgresqlExtDatabase('civis')

class BaseModel(Model):
	class Meta:
		database = db

class Route(BaseModel):
	route_id = CharField()
	stop_list = CharField(max_length=4000)
	route_length = IntegerField()
	json_route = JSONField()

class Stop(BaseModel):
	stop_id = IntegerField()
	routes = CharField(max_length = 400)
	on_street = CharField()
	cross_street = CharField()
	location = HStoreField()

class Stop_Snapshot(BaseModel):
	stop_fk = ForeignKeyField(Stop)
	boardings = DecimalField()
	alightings = DecimalField()
	month_begining = DateField()

class Lookup_Route_Stop (BaseModel):
	stop_id = ForeignKeyField(Stop, related_name=stopXREF)
	route_id = ForeignKeyField(Route, related_name=RouteXREF)

def create_tables():
	db.connect
	db.create_tables([Route, Stop, Stop_Snapshot, Lookup_Route_Stop])

def load_bus_data():
	r = requests.get('http://data.cityofchicago.org/resource/mq3i-nnqe.json')

	if r.status_code == requests.codes.ok:
		allRoutes = {}

		for x in r.json():

			#etl - Load Stops
			s = Stop()
			s.stop_id = x['stop_id']
			s.routes = x['routes']
			s.on_street = x['on_street']
			s.cross_street = x['cross_street']
			s.location = x['location']
			#the way this boolean is saved in source trips up playhouse
			s.location['needs_recoding'] = str(s.location['needs_recoding'])
			s.save()

			#load stop snapshot
			snap = Stop_Snapshot.create(
				stop_fk = s,
				boardings = x['boardings'],
				alightings = x['alightings'],
				month_begining = x['month']
				)
			snap.save()

			
			
			for r in x['routes'].split(','):
				if r <> '':
					if r in allRoutes:
						allRoutes[r][s.stop_id] = s.location
					else:
						allRoutes[r] = {s.stop_id:s.location}

		for key in allRoutes:
			route = Route.create(
				route_id = key,
				stop_list = str(allRoutes[key].keys()),
				route_length = int(len(allRoutes[key])),
				json_route = {'route_id':key, 'stop_list':allRoutes[key]}
				)

			#load stop / route lookup	
			#build the lookup
			for skey in allRoutes[key]:
				lookup_rs = Lookup_Route_Stop.create(
					stop_id = Stop.get(Stop.stop_id == skey),
					route_id = route
				)










>>> Person.create_table()
>>> Address.create_table()

--Install this for the orm! psycopg2
--Using the peewee orm
longestRoute = (Route.select().order_by(Route.route_length.desc()).get())
