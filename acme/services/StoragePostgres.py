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

        # Connect to postgres DB
        self._connection = psycopg2.connect(database="acme-cse", host="localhost", user="postgres", password="musang")
        # Open a cursor to perform database operations
        self._cursor = self._connection.cursor()
        
    def closeConnection(self):
        # Close cursor and connection to databse
        self._cursor.close()
        self._connection.close()
        
    #
    #	Resources
    #

    def insertResource(self, resource: Resource) -> None:
        # self.tabResources.insert(resource.dict)
        self._cursor.execute(resource.getInsertQuery())
        self._connection.commit()
    

    def upsertResource(self, resource: Resource) -> None:
        self.insertResource(resource)
        # #L.logDebug(resource)
        # with self.lockResources:
        # 	# Update existing or insert new when overwriting
        # 	# self.tabResources.upsert(resource.dict, self.resourceQuery.ri == resource.ri)
        #     pass
    

    def updateResource(self, resource: Resource) -> Resource:
        #L.logDebug(resource)
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
        # self.tabResources.remove(self.resourceQuery.ri == resource.ri)	
        pass
    

    def searchResources(self, ri:Optional[str] = None, 
                              csi:Optional[str] = None, 
                              srn:Optional[str] = None, 
                              pi:Optional[str] = None, 
                              ty:Optional[int] = None, 
                              aei:Optional[str] = None) -> list[Document]:

        # TODO: For resource that have ontologyRef (eg. cnt and ae), in DB it's not in shortname instead ontologyref

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


    # def discoverResourcesByFilter(self, func:Callable[[JSON], bool]) -> list[Document]:
    #     # Maybe result that already in dict, passed to func callable. func expect JSON type which is Dict[str, Any]
    #     # return self.tabResources.search(func)	# type: ignore [arg-type]
    #     pass
    
    def retrieveOldestResource(self, ty: int, pi:Optional[str] = None) -> Optional[dict]:
        # Get shortname of resources type
        rType = ResourceTypes(ty).tpe()
        tyShortName = rType.split(":")[1]
        # Format and execute query
        query = """
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, {} WHERE resources.ty = {} AND resources.index = {}.resource_index {}ORDER BY resources.ct LIMIT 1
                ) as results;
                """
        query = query.format(tyShortName, ty, tyShortName, (f"AND resources.pi='{pi}' " if pi != None else ""))
        result = self._execQuery(query)
        
        return (result[0] if len(result) > 0 else None)
    
        # TODO: Fix self._execQuery in this file that directly access index 0, when execQuery return empty list. if return not expect list but None, set type check to Optional
        
    
    def retrieveLatestResource(self, ty: int, pi:Optional[str] = None) -> Optional[dict]:
        # Get shortname of resources type
        rType = ResourceTypes(ty).tpe()
        tyShortName = rType.split(":")[1]
        # Format and execute query
        query = """
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, {} WHERE resources.ty = {} AND resources.index = {}.resource_index {}ORDER BY resources.lt DESC LIMIT 1
                ) as results;
                """
        query = query.format(tyShortName, ty, tyShortName, (f"AND resources.pi='{pi}' " if pi != None else ""))
        result = self._execQuery(query)
        
        return (result[0] if len(result) > 0 else None)


    def hasResource(self, ri:Optional[str] = None, 
                          csi:Optional[str] = None, 
                          srn:Optional[str] = None,
                          ty:Optional[int] = None) -> bool:        
        query = "SELECT EXISTS (SELECT 1 FROM {} WHERE {} = {} LIMIT 1);"
        
        if ri:
            query = query.format("resources", "ri", f"'{ri}'")
        elif ty:
            query = query.format("resources", "ty", ty)
        elif srn:
            query = query.format("resources", "__srn__", f"'{srn}'")
        elif csi:
            query = query.format("cb", "csi", f"'{csi}'")
        
        return self._execQuery(query)[0]


    def countResources(self) -> int:
        query = "SELECT COUNT(*) FROM resources;"
        return self._execQuery(query)[0]

        
    def countResourcesBy(self, pi:str, ty:Optional[ResourceTypes] = None) -> int:
        # TODO: This is not a pythonic way to write
        query = f"SELECT COUNT(*) FROM resources WHERE pi = '{pi}'"
        if ty != None:
            query = query + f" AND ty = {ty}"
        query = query + ";"

        return self._execQuery(query)[0]


    def searchByFragment(self, dct:dict) -> list[Document]:
        """ Search and return all resources that match the given dictionary/document. """
        # return self.tabResources.search(self.resourceQuery.fragment(dct))
        pass
    
    def retrieveResourceBy(self, acpi: Optional[str] = None, 
                           ty: Optional[int] = None, 
                           filterResult: Optional[list] = None) -> Optional[list[JSON]]:
        """ Retrieve list of resource in dict based on attribute value

        Args:
            acpi (Optional[str], optional): resources to search that have ACP ri in acpi attribute value. Defaults to None.
            ty (Optional[int], optional): resources to search that match ty or to help query when need to retrieve specific resource attribute. Defaults to None.
            filterResult (Optional[list], optional): list of attribute needs to retrieve. Defaults to None.

        Returns:
            Optional[list[JSON]]: list of resource in specific filter or all attributes
        """        
        
        # TODO: Currently only specific used by ACP.deactivate(). For further development, make this a general purpose function
        
        query = ""
        
        if acpi:
            query = f"""
                    SELECT row_to_json(results) FROM (
                        SELECT ri, acpi, __rtype__ FROM resources WHERE acpi @> '[\"{acpi}\"]'
                    ) as results;
                    """
        
        if query != "":
            result = self._execQuery(query)
            return result if result != [] else None
            
        return None


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
        
        # TODO: Optimize query call by using pgsql loop in the query?
        result = []
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
        # Get shortname of resources type 
        rType = ResourceTypes(ty).tpe()
        tyShortName = rType.split(":")[1]
        # Format query
        query = """
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, {} WHERE resources.pi = '{}' AND resources.ty = {} AND resources.index = {}.resource_index
                ) as results;
                """
        query = query.format(tyShortName, pi, ty, tyShortName)
        
        return self._execQuery(query)
    

    def _selectByTY(self, ty: int) -> list:
        # Get shortname of resources type 
        rType = ResourceTypes(ty).tpe()
        tyShortName = rType.split(":")[1]
        # Format query
        query = f"""
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, {tyShortName} WHERE resources.ty = {ty} AND resources.index = {tyShortName}.resource_index
                ) as results;
                """

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
        
        # Return merge data from resources table and specific resource type table
        return [( baseResult[0] | resourceResult[0] )]
    

    def _execQuery(self, query: str) -> list:
        # TODO: Remove newline from query string
        self._cursor.execute(query)
        rows = self._cursor.fetchall()
        result = []
        for row in rows:
            result.append(row[0])

        return result
    

if __name__ == "__main__":
    binding = PostgresBinding()
    # print( binding.retrieveOldestResource(3) )
    # print( binding.searchResources(ty=5) )
    # print( binding.searchResources(pi = "cse1234", ty=1) )
    # print( binding.retrieveLatestResource(ty=1,pi="cse1234") )
    
    print( binding.retrieveResourceBy(acpi="acp777") )
    
    binding.closeConnection()
    
    