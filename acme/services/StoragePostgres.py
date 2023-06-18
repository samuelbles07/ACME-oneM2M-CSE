from __future__ import annotations
from typing import Callable, cast, List, Optional

import os, shutil
from threading import Lock
import psycopg2

from ..etc.Types import ResourceTypes, Result, ResponseStatusCode, JSON
from ..etc import DateUtils
from ..services.Configuration import Configuration
from ..services import CSE
from ..resources.Resource import Resource
from ..resources import Factory
from ..services.Logging import Logging as L


class PostgresBinding():
    def __init__(self) -> None:
        L.isInfo and L.log("Initialize postgres binding!")

        # create transaction locks
        self.lockResources = Lock()

        # Connect to your postgres DB
        self.connection = psycopg2.connect(database="acme-cse", host="localhost", user="postgres", password="musang")
        # Open a cursor to perform database operations
        self.cursor = self.connection.cursor()

    #
	#	Resources
	#

	def insertResource(self, resource: Resource) -> None:
		with self.lockResources:
			# self.tabResources.insert(resource.dict)
            self.cursor.execute(resource.getInsertQuery())
            self.connection.commit()
	

	def upsertResource(self, resource: Resource) -> None:
        self.insertResource(resource)
		# #L.logDebug(resource)
		# with self.lockResources:
		# 	# Update existing or insert new when overwriting
		# 	# self.tabResources.upsert(resource.dict, self.resourceQuery.ri == resource.ri)
        #     pass
	

	def updateResource(self, resource: Resource) -> Resource:
		#L.logDebug(resource)
		with self.lockResources:
			# ri = resource.ri
			# self.tabResources.update(resource.dict, self.resourceQuery.ri == ri)
			# # remove nullified fields from db and resource
			# for k in list(resource.dict):
			# 	if resource.dict[k] is None:	# only remove the real None attributes, not those with 0
			# 		self.tabResources.update(delete(k), self.resourceQuery.ri == ri)	# type: ignore [no-untyped-call]
			# 		del resource.dict[k]
			# return resource
            pass


	def deleteResource(self, resource:Resource) -> None:
		with self.lockResources:
			# self.tabResources.remove(self.resourceQuery.ri == resource.ri)	
            pass
	

	def searchResources(self, ri:Optional[str] = None, 
							  csi:Optional[str] = None, 
							  srn:Optional[str] = None, 
							  pi:Optional[str] = None, 
							  ty:Optional[int] = None, 
							  aei:Optional[str] = None) -> list[Document]:

        """
        For return as document, create dict that already mapped from query result (1), then create Document(dict) object from tiny db
        (1) has a problem, so it needs to somehow mapped based on every resource type and if field value is null, then don't include the field.
        Other problem is, what mapFunction from what resource needs to be called? 

        Or just refactor everything that return tinydb.Document and Result with raw resource
        """
        # SELECT JSON_AGG(resources) as resources, JSON_AGG(acp) as acp FROM resources, acp WHERE resources.ri = 'acp1234' AND resources.index = acp.resource_index;
        # TODO: For result that return multiple data, somehow need to map result from resource table query and resource type specific query
        # TODO: For resource that have ontologyRef (eg. cnt and ae), in DB it's not in shortname instead ontologyref
        # SELECT JSON_AGG(resources) FROM resources WHERE ri = 'acp1234';
        # select JSON_AGG(acp) FROM acp WHERE resource_index = resources.index;
        # for type name, get __rtype__ separator by ':' and get index 1



        baseQuery:str = ""

        # if ri:

        # elif pi and ty:
        #     # parentId and resourceType provided
        #     # NOTE: Can do it in 1 query
            
        # elif pi:
        # elif ty:
        #     # NOTE: Can do it in 1 query
        # elif aei:
        #     # NOTE: Can do it in 1 query
        # elif csi:
        #     # NOTE: Can do it in 1 query
            


        return []


	def discoverResourcesByFilter(self, func:Callable[[JSON], bool]) -> list[Document]:
        # TODO: In here, how to apply it? biatch???
        # Maybe result that already in dict, passed to func callable. func expect JSON type which is Dict[str, Any]
		with self.lockResources:
			# return self.tabResources.search(func)	# type: ignore [arg-type]
            pass


	def hasResource(self, ri:Optional[str] = None, 
						  csi:Optional[str] = None, 
						  srn:Optional[str] = None,
						  ty:Optional[int] = None) -> bool:
		# if not srn:
		# 	with self.lockResources:
		# 		if ri:
		# 			return self.tabResources.contains(self.resourceQuery.ri == ri)	
		# 		elif csi :
		# 			return self.tabResources.contains(self.resourceQuery.csi == csi)
		# 		elif ty is not None:	# ty is an int
		# 			return self.tabResources.contains(self.resourceQuery.ty == ty)
		# else:
		# 	# find the ri first and then try again recursively
		# 	if len((identifiers := self.searchIdentifiers(srn=srn))) == 1:
		# 		return self.hasResource(ri = identifiers[0]['ri'])
		# return False
        pass


	def countResources(self) -> int:
		with self.lockResources:
			# return len(self.tabResources)
            pass


	def searchByFragment(self, dct:dict) -> list[Document]:
		""" Search and return all resources that match the given dictionary/document. """
		with self.lockResources:
			# return self.tabResources.search(self.resourceQuery.fragment(dct))
            pass

    def _selectByRI(self, ri: str) -> list:
        # NOTE: Execute query 2 times. first from table resource, 2nd from table specific resourcetype
        baseQuery = "SELECT row_to_json(resources) FROM resources WHERE ri = '{}'" + ri
        
    def _selectByPI(self, pi: str) -> list:
        # NOTE: Execute query 2 times
        baseQuery = "SELECT row_to_json(resources) FROM resources WHERE pi = '{}'".format(pi)
        pass

    def _selectByPI(self, pi: str, ty: int) -> list:
        # NOTE: Can do this only 1 query, by retrieve ty name from ResourceTypes class
        baseQuery = "SELECT row_to_json(resources) FROM resources WHERE pi = '{}' AND ty = {}".format(pi, ty)
        pass

    def _selectByTY(self, ty: int) -> list:
        # NOTE: Can do this only 1 query, by retrieve ty name from ResourceTypes class
        baseQuery = "SELECT row_to_json(resources) FROM resources WHERE ty = {}".format(ty)
        pass
    
    def _selectByCSI(self, csi: str) -> list:
        # NOTE: Can do this only 1 query, because it directly select from resource type
        baseQuery = "SELECT row_to_json(cse) FROM cse WHERE csi = {}".format(csi)
        pass
    
    def _selectByAEI(self, aei: str) -> list:
        # NOTE: Can do this only 1 query, because it directly select from resource type
        baseQuery = "SELECT row_to_json(ae) FROM ae WHERE aei = '{}'".format(aei)
        pass

    def _execQuery(self, query: str) -> list:
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        result = []
        for row in rows:
            result.append(row[0])

        return result
