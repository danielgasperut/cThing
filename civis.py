#http://data.cityofchicago.org/resource/mq3i-nnqe.json
from flask import *
from playhouse.postgres_ext import *
import requests

#setup app
app = Flask(__name__)
app.config.from_object(__name__)

#setup Db
db = PostgresqlExtDatabase('civis')

class BaseModel(Model):
	class Meta:
		database = db

class Route(BaseModel):
	route_id = CharField()
	stop_list = CharField(max_length=4000)
	route_length = IntegerField()
	json_route = JSONField()

	#find the longest route
	def get_longest_route():
		return Route.select().order_by(Route.route_length.desc()).get()

class Stop(BaseModel):
	stop_id = IntegerField()
	routes = CharField(max_length = 400, null=True)
	on_street = CharField()
	cross_street = CharField()
	location = HStoreField()


class Stop_Snapshot(BaseModel):
	stop_fk = ForeignKeyField(Stop)
	boardings = DecimalField()
	alightings = DecimalField()
	month_begining = DateField()

class Lookup_Route_Stop (BaseModel):
	stop = ForeignKeyField(Stop, related_name='stopXREF')
	route = ForeignKeyField(Route, related_name='RouteXREF')

	def get_most_connected_stop():
		return Lookup_Route_Stop.get()

#function to setup the db
def create_tables():
	db.connect
	db.create_tables([Route, Stop, Stop_Snapshot, Lookup_Route_Stop])

#function to load the bus data from external source
def load_bus_data():
	r = requests.get('https://data.cityofchicago.org/resource/mq3i-nnqe.json?$limit=12000', headers={'X-App-Token': '1qBsY5fMYDUlsqdHXN4ULAD7d'} )

	if r.status_code == requests.codes.ok:
		allRoutes = {}

		for x in r.json():

			#etl - Load Stops

			s = Stop()
			s.stop_id = x['stop_id']
			#sanitize inputs (some null routes)
			if 'routes' in x:
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

			
			#build a route array as we populate each stop
			#sanitize inputs (some null routes)
			if 'routes' in x:
				for i in x['routes'].split(','):
					i = i.strip()
					if i <> '':
						if i in allRoutes:
							allRoutes[i][s.stop_id] = s.location
						else:
							allRoutes[i] = {s.stop_id:s.location}
		#load routes
		for key in allRoutes:
			route = Route.create(
				route_id = key,
				stop_list = str(allRoutes[key].keys()),
				route_length = int(len(allRoutes[key])),
				json_route = {'route_id':key, 'stop_list':allRoutes[key]}
				)
			route.save()

			#load stop / route lookup	
			#build the lookup
			for skey in allRoutes[key]:
				lookup_rs = Lookup_Route_Stop.create(
					stop = Stop.get(Stop.stop_id == skey),
					route = route
				)
				lookup_rs.save()




# allow running from the command line
if __name__ == '__main__':
    app.run()





#>>> Person.create_table()
#>>> Address.create_table()

#--Install this for the orm! psycopg2
#--Using the peewee orm
#longestRoute = (Route.select().order_by(Route.route_length.desc()).get())
