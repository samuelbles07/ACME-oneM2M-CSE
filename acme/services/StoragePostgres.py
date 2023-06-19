from __future__ import annotations
from typing import Callable, cast, List, Optional

import os, shutil
from threading import Lock
import psycopg2
from tinydb.table import Document

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

        """
        SELECT row_to_json(results) FROM (
            SELECT * FROM resources, acp WHERE resources.ty = 1 AND resources.index = acp.resource_index
        ) as results;
        """

        if ri:
            return self._selectByRI(ri = ri)
        elif pi and ty:
            return self._selectByPI(pi = pi, ty = ty)
        elif pi:
            return self._selectByPI(pi = pi)
        elif ty:
            return self._selectByTY(ty = ty)
        elif aei:
            return self._selectByAEI(aei = aei)
        elif csi:
            return self._selectByCSI(csi = csi)
        elif srn:
            return self._selectBySRN(srn = srn)

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

    def _selectByRI(self, ri: str) -> list[dict]:
        """Expected only return 1 value, because resource identifier is unique

        Args:
            ri (str): resource RI that want to retrieve

        Returns:
            list[dict]: resource attribute in list of dict. List only have 1 data.
        """
        
        # Retrieve data from resources table
        query = "SELECT row_to_json(resources) FROM resources WHERE ri = '{}'".format(ri)
        baseResult = self._execQuery(query)
        
        # Do check if resource exist
        if len(baseResult) == 0:
            return []
        
        # Get resource type name in shortname for table name reference
        rtype = baseResult[0]["__rtype__"]
        tyShortName = rtype.split(":")[1]
        
        # Retrieve data from target resource type table
        query = "SELECT row_to_json({}) FROM {} WHERE resource_index = '{}'".format(tyShortName, tyShortName, baseResult[0]["index"])
        resourceResult = self._execQuery(query)
        
        # TODO: If resourceResult is empty, return empty
        
        # Merge dict into 1 dictionary and append to list
        result = []
        result.append( baseResult[0] | resourceResult[0] )
        
        return result
        
        
    def _selectByPI(self, pi: str) -> list[dict]:
        """ Return list of resource that parentId match

        Args:
            pi (str): target parent id that want to retrieve

        Returns:
            list[dict]: list of resources that parentId is match or empty list
        """
        
        query = "SELECT row_to_json(resources) FROM resources WHERE pi = '{}'".format(pi)
        baseResult = self._execQuery(query)
        
        # Do check if resource exist
        if len(baseResult) == 0:
            return []
        
        result = []

        # TODO: How to map dict from resources table and "resource type" table
        
        for base in baseResult:
            # Get resource type name in shortname for table name reference
            rtype = base["__rtype__"]
            tyShortName = rtype.split(":")[1]
            
            # Retrieve data from target resource type table
            query = "SELECT row_to_json({}) FROM {} WHERE resource_index = '{}'".format(tyShortName, tyShortName, base["index"])
            resourceResult = self._execQuery(query)
            # TODO: If query result is empty, don't append to result
            result.append( base | resourceResult[0] )
        
        return result


    def _selectByPI(self, pi: str, ty: int) -> list[dict]:
        query = """
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, {} WHERE resources.pi = '{}' AND resources.ty = {} AND resources.index = {}.resource_index
                ) as results;
                """
         
        # TODO: Get ty from ResourceType class       
        if ty == 1:
            query = query.format("acp", pi, ty, "acp")
        elif ty == 2:
            query = query.format("ae", pi, ty, "ae")
        elif ty == 5:
            query = query.format("cb", pi, ty, "cb")

        return self._execQuery(query)

    def _selectByTY(self, ty: int) -> list:
        query = """
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, {} WHERE resources.ty = {} AND resources.index = {}.resource_index
                ) as results;
                """
         
        # TODO: Get ty from ResourceType class       
        if ty == 1:
            query = query.format("acp", ty, "acp")
        elif ty == 2:
            query = query.format("ae", ty, "ae")
        elif ty == 5:
            query = query.format("cb", ty, "cb")

        return self._execQuery(query)
    
    def _selectByCSI(self, csi: str) -> list[dict]:
        # NOTE: Can do this only 1 query, because it directly select from resource type
        query = """
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, cb WHERE cb.csi = '{}' AND resources.index = cb.resource_index
                ) as results;
                """.format(csi)
        return self._execQuery(query)

    
    def _selectByAEI(self, aei: str) -> list[dict]:
        query = """
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, ae WHERE ae.aei = '{}' AND resources.index = ae.resource_index
                ) as results;
                """.format(aei)
        return self._execQuery(query)
    
    def _selectBySRN(self, srn: str) -> list[dict]:
        # Retrieve data from resources table
        query = "SELECT row_to_json(resources) FROM resources WHERE __srn__ = '{}'".format(srn)
        baseResult = self._execQuery(query)
        
        # Do check if resource exist
        if len(baseResult) == 0:
            return []
        
        # Get resource type name in shortname for table name reference
        rtype = baseResult[0]["__rtype__"]
        tyShortName = rtype.split(":")[1]
        
        # Retrieve data from target resource type table
        query = "SELECT row_to_json({}) FROM {} WHERE resource_index = '{}'".format(tyShortName, tyShortName, baseResult[0]["index"])
        resourceResult = self._execQuery(query)
        
        # TODO: If resourceResult is empty, return empty
        
        # Merge dict into 1 dictionary and append to list
        result = []
        result.append( baseResult[0] | resourceResult[0] )
        
        return result

    def _execQuery(self, query: str) -> list:
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        result = []
        for row in rows:
            result.append(row[0])

        return result
    
