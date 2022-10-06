from sqlalchemy import Column, ForeignKey, Integer, Float, String, create_engine
from sqlalchemy.orm import declarative_base, relationship
import pandas as pd
from sqlalchemy.orm import Session


Base = declarative_base()

class Route(Base):
	__tablename__ = "route"
	
	id = Column(Integer, primary_key=True)

	start_point_name = Column(String)
	start_point_esr = Column(Integer)
	start_point_dor = Column(Integer)
	start_point_okato = Column(String)
	start_point_x = Column(Float)
	start_point_y = Column(Float)

	end_point_name = Column(String)
	end_point_esr = Column(Integer)
	end_point_dor = Column(Integer)
	end_point_okato = Column(String)
	end_point_x = Column(Float)
	end_point_y = Column(Float)

	route_units = relationship(
		"RouteUnit", cascade="all, delete-orphan"
		)

class RouteUnit(Base):
	__tablename__ = "route_unit"

	id = Column(Integer, primary_key=True)

	start_point_name = Column(String)
	start_point_esr = Column(Integer)
	start_point_dor = Column(Integer)
	start_point_okato = Column(String)

	end_point_name = Column(String)
	end_point_esr = Column(Integer)
	end_point_dor = Column(Integer)
	end_point_okato = Column(String)

	route_id = Column(Integer, ForeignKey("route.id"))
	route = relationship("Route", back_populates="route_units")


if __name__ == '__main__':
	# Вообще это всё лучше тянуть из конфигов, но тут я оставил обычной строкой для простоты
	engine = create_engine('postgresql://postgres:42134@localhost/routes_db')
	Base.metadata.create_all(engine)
	df = pd.read_excel('~/Downloads/Telegram Desktop/razb_uch.xlsx')

	routes_df = df.iloc[:, 0:13]
	routes_df = routes_df.drop_duplicates(subset=['ID_UCH_VOST_POL'])

	route_units_df = df.iloc[:, 13:]
	route_units_df['ROUTE_ID'] = df.iloc[:, [0]]

	with Session(engine) as session:
		routes = []
		route_units = []
		for _, row in routes_df.iterrows():
			route = Route(
				start_point_name=row['NAME_BEGIN_VOST_UCH'],
				start_point_esr=row['ESR_BEGIN_VOST_UCH'],
				start_point_dor=row['DOR_BEGIN_VOST_UCH'],
				start_point_okato=row['OKATO_BEGIN_VOST_UCH_NAME'],
				start_point_x = row['X_BEG_VOST_UCH'],
				start_point_y = row['Y_BEG_VOST_UCH'],
				end_point_name = row['NAME_END_VOST_UCH'],
				end_point_esr = row['ESR_END_VOST_UCH'],
				end_point_dor = row['DOR_END_VOST_UCH'],
				end_point_okato = row['OKATO_END_VOST_UCH_NAME'],
				end_point_x = row['X_END_VOST_UCH'],
				end_point_y = row['Y_END_VOST_UCH'],
				)
			routes.append(route)

		for _, row in route_units_df.iterrows():
			route_unit = RouteUnit(
				start_point_name=row['NAME_BEGIN_MELK_SET'],
				start_point_esr=row['ESR_BEGIN_MELK_SET'],
				start_point_dor=row['DOR_BEGIN_MELK_SET'],
				start_point_okato=row['OKATO_BEGIN_MELK_SET_NAME'],
				end_point_name=row['NAME_END_MELK_SET'],
				end_point_esr=row['ESR_END_MELK_SET'],
				end_point_dor=row['DOR_END_MELK_SET'],
				end_point_okato=row['OKATO_END_MELK_SET_NAME'],
				route_id=row['ROUTE_ID'],
				)
			route_units.append(route_unit)

		session.add_all(routes)
		session.add_all(route_units)
		session.commit()

	print(route_units_df)
