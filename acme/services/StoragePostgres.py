from __future__ import annotations
from typing import Callable, cast, List, Optional, Tuple

import os, shutil
from threading import Lock
import psycopg2

from ..etc.Types import ResourceTypes, Result, ResponseStatusCode, JSON
from ..etc import DateUtils, Utils
from ..services.Configuration import Configuration
from ..services import CSE
from ..resources.Resource import Resource
from ..resources import Factory
from ..services.Logging import Logging as L

# TODO: Because of threading, and only using 1 connetion pool. If 2 request at the time happen to DB, error occur: Failed exec query: the connection cannot be re-entered recursively


class PostgresBinding():
    def __init__(self) -> None:
        L.isInfo and L.log("Initialize postgres binding!")
        # Connect to postgres DB, will crash if failed connect
        self._connection = psycopg2.connect(database="acme-cse", 
                                            host=Configuration.get("db.hostname"), 
                                            port= Configuration.get("db.port"), 
                                            user=Configuration.get("db.username"), 
                                            password=Configuration.get("db.password"))
        # Open a cursor to perform database operations
        # self._cursor = self._connection.cursor()
        L.isInfo and L.log('Postgres connection initialized')
        
        self._lockExecution = Lock()
        
    def closeConnection(self):
        # Close cursor and connection to databse
        # self._cursor.close()
        with self._lockExecution:
            self._connection.close()
            L.isInfo and L.log('Postgres connection closed')
        
    ##############################################################################
    #
    #	Resource and specific resource type table Implementation
    #

    def insertResource(self, resource: Resource) -> bool:
        query = resource.getInsertQuery()
        if query == None:
            return False
        
        return self._execManipulationQuery(query)
    

    def upsertResource(self, resource: Resource) -> None:
        self.insertResource(resource)
        # #L.logDebug(resource)
        # with self.lockResources:
        # 	# Update existing or insert new when overwriting
        # 	# self.tabResources.upsert(resource.dict, self.resourceQuery.ri == resource.ri)
        #     pass
    

    def updateResource(self, resource: Resource) -> bool:
        query = resource.getUpdateQuery()
        if query == None:
            return False
        
        return self._execManipulationQuery(query)


    def deleteResource(self, resource:Resource) -> bool:
        query = f"DELETE FROM public.resources WHERE ri = '{resource.ri}';"
        return self._execManipulationQuery(query)
    

    def searchResources(self, ri:Optional[str] = None, 
                              csi:Optional[str] = None, 
                              srn:Optional[str] = None, 
                              pi:Optional[str] = None, 
                              ty:Optional[int] = None, 
                              aei:Optional[str] = None) -> list[JSON]:

        # TODO: For resource that have ontologyRef (eg. cnt and ae), in DB it's not in shortname instead ontologyref

        if ri and ty:
            return self._selectByRI(ri = ri, ty = ty)
        elif ri:
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
    
    
    def retrieveResourceByAttribute(self, acpi: Optional[str] = None, 
                                    mid: Optional[str] = None,
                                    ty: Optional[int] = None,
                                    mcsi: Optional[str] = None, 
                                    filter:Optional[Callable[[JSON], bool]] = None) -> list[JSON]:
        result = []
        if acpi:
            result = self._selectByACPI(acpi)
        elif mid:
            result = self._selectByMID(mid)
        if mcsi:
            result = self._selectByMCSI(mcsi, filter)
            
        return result
    
    
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
    
    
    def retrieveExpiredResource(self) -> list[JSON]:
        query = f"""
                    SELECT row_to_json(results) FROM (
                       SELECT * FROM resources WHERE et < now()
                    ) as results;
                    """
        baseResult = self._execQuery(query)
        
        # TODO: Optimize query call by using pgsql loop in the query?
        result = []
        for base in baseResult:
            # Get resource type name in shortname for table name reference
            rtype = base["__rtype__"]
            tyShortName = rtype.split(":")[1]
            
            # Retrieve data from target resource type table
            query = "SELECT row_to_json({}) FROM {} WHERE resource_index = {}".format(tyShortName, tyShortName, base["index"])
            resourceResult = self._execQuery(query)
            # If query result is not empty, append to result. TODO: if it empty, somehow it is inconsistent. Maybe delete it from resource table.
            if len(resourceResult) > 0:
                result.append( base | resourceResult[0] )
            
        return result


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
        
        result = self._execQuery(query)
        return result[0] if len(result) > 0 else False


    def countResources(self, ty: Tuple[ResourceTypes, ...] = None) -> int:
        query = ""
        if ty == None:
            query = "SELECT COUNT(*) FROM resources;"
        else:
            # Loop through tuple to build the query
            query = f'SELECT COUNT(*) FROM resources WHERE ty = { int(ty[0]) } '
            for i, t in enumerate(ty):
                if i == 0:
                    continue
                query += f'OR ty = { int(t) } '

        result = self._execQuery(query)
        return result[0] if len(result) > 0 else 0

        
    def countResourcesBy(self, pi:str, ty:Optional[ResourceTypes] = None) -> int:
        # TODO: This is not a pythonic way to write
        query = f"SELECT COUNT(*) FROM resources WHERE pi = '{pi}'"
        if ty != None:
            query = query + f" AND ty = {ty}"
        query = query + ";"
        result = self._execQuery(query)
        return result[0] if len(result) > 0 else 0


    def searchByFragment(self, dct:dict) -> list[JSON]: 
        """ Search and return all resources that match the given dictionary/document. """
        # return self.tabResources.search(self.resourceQuery.fragment(dct))
        pass
    
    
    def searchIdentifiers(self, ri:Optional[str] = None, 
								srn:Optional[str] = None) -> list[JSON]:
        """	Search for an resource ID OR for a structured name

			Either *ri* or *srn* shall be given. If both are given then *srn*
			is taken.
		
			Args:
				ri: Resource ID to search for.
				srn: Structured path to search for.
			Return:
				A list of found identifier data (ri, rn, srn, ty)
		 """
        query = "SELECT row_to_json(results) FROM ( SELECT ri, rn, __srn__, ty FROM resources WHERE {}) as results;"
        if srn:
            query = query.format(f"__srn__='{srn}'")
        elif ri:
            query = query.format(f"ri='{ri}'")
        else:
            L.isDebug and L.logDebug("postgres.searchIdentifiers() parameter both not provided")
            return []
        result = self._execQuery(query)
        
        # Loop through result and replace keyname to without internal prefix (__key__)
        for i, _ in enumerate(result):
            result[i]["srn"] = result[i]["__srn__"]
            del result[i]["__srn__"]
        
        return result
    
    
    ##############################################################################
    #
    #	Batch Notification Table Implementation
    #
    
    def addBatchNotification(self, ri:str, nu:str, notificationRequest:JSON) -> bool:
        query = "INSERT INTO public.batch_notif(ri, nu, tstamp, request) VALUES ({}, {}, {}, {});"
        query = query.format(
            Utils.validateAttributeValue(ri),
            Utils.validateAttributeValue(nu),
            Utils.validateAttributeValue(DateUtils.getResourceDate()),
            Utils.validateAttributeValue(notificationRequest)
        )
        return self._execManipulationQuery(query)


    def countBatchNotifications(self, ri:str, nu:str) -> int:
        query = "SELECT COUNT(*) FROM public.batch_notif;"
        result = self._execQuery(query)
        return result[0] if len(result) > 0 else 0
    

    def getBatchNotifications(self, ri:str, nu:str) -> list[JSON]:
        query = f"""
                SELECT row_to_json(results) FROM (
                    SELECT ri, nu, DATE_PART('epoch', tstamp) AS tstamp, request FROM batch_notif WHERE ri='{ri}' AND nu='{nu}'
                ) as results;
                """
        # query = f"SELECT row_to_json(batch_notif) FROM batch_notif WHERE ri='{ri}' AND nu='{nu}';"
        return self._execQuery(query)
    

    def removeBatchNotifications(self, ri:str, nu:str) -> bool:
        query = f"DELETE FROM public.batch_notif WHERE ri='{ri}' AND nu='{nu}';"
        return self._execManipulationQuery(query)

    
    ##############################################################################
    #
    #	Private Implementation
    #

    def _selectByRI(self, ri: str, ty: Optional[int] = None) -> list[dict]:
        """Expected only return 1 value, because resource identifier is unique

        Args:
            ri (str): resource RI that want to retrieve

        Returns:
            list[dict]: resource attribute in list of dict. List only have 1 data.
        """
        if ty:
            # Get shortname of resources type 
            rType = ResourceTypes(ty).tpe()
            tyShortName = rType.split(":")[1]
            # Format query
            query = """
                    SELECT row_to_json(results) FROM (
                        SELECT * FROM resources, {} WHERE resources.ri = '{}' AND resources.ty = {} AND resources.index = {}.resource_index
                    ) as results;
                    """
            query = query.format(tyShortName, ri, ty, tyShortName)
            return self._execQuery(query)
        
        
        # TODO: Do it in 1 query, result _rtype_ as reference table to look for
        # Retrieve data from resources table
        query = "SELECT row_to_json(resources) FROM resources WHERE ri = '{}'".format(ri)
        baseResult = self._execQuery(query)
        
        # Do check if resource exist
        if len(baseResult) == 0:
            return []
        
        # Because virtual resource don't have dedicated attribute, it doesn't have it's own table
        if baseResult[0]["__isvirtual__"]:
            return baseResult
        
        # Get resource type name in shortname for table name reference
        rtype = baseResult[0]["__rtype__"]
        tyShortName = rtype.split(":")[1]
        
        # Retrieve data from target resource type table
        query = "SELECT row_to_json({}) FROM {} WHERE resource_index = {}".format(tyShortName, tyShortName, baseResult[0]["index"])
        resourceResult = self._execQuery(query)
        
        result = []
        # If query result is not empty, append to result. TODO: if it empty, somehow it is inconsistent. Maybe delete it from resource table.
        if len(resourceResult) > 0:
            # merge data from resources table and specific resource type table
            result.append( baseResult[0] | resourceResult[0] )
        
        return result
        
        
    def _selectByPI(self, pi: str, ty: Optional[int] = None) -> list[dict]:
        """ Return list of resource that parentId match

        Args:
            pi (str): target parent id that want to retrieve
            ty (Optional[int]): filter search with type. Default None

        Returns:
            list[dict]: list of resources that parentId is match or empty list
        """
        if ty:
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
        
        # No reference what the resource type of the ri
        query = "SELECT row_to_json(resources) FROM resources WHERE pi = '{}'".format(pi)
        baseResult = self._execQuery(query)
        
        # Do check if resource exist
        if len(baseResult) == 0:
            return []
        
        # TODO: Optimize query call by using pgsql loop in the query?
        result = []
        for base in baseResult:
            # Because virtual resource don't have dedicated attribute, it doesn't have it's own table
            if base["__isvirtual__"]:
                result.append(base)
                continue
            
            # Get resource type name in shortname for table name reference
            rtype = base["__rtype__"]
            tyShortName = rtype.split(":")[1]
            
            # Retrieve data from target resource type table
            query = "SELECT row_to_json({}) FROM {} WHERE resource_index = {}".format(tyShortName, tyShortName, base["index"])
            resourceResult = self._execQuery(query)
            # If query result is not empty, append to result. TODO: if it empty, somehow it is inconsistent. Maybe delete it from resource table.
            if len(resourceResult) > 0:
                # merge data from resources table and specific resource type table
                result.append( base | resourceResult[0] )
        
        return result
    

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
        
        # Because virtual resource don't have dedicated attribute, it doesn't have it's own table
        if baseResult[0]["__isvirtual__"]:
            L.logDebug(f'baseResult: {baseResult}')
            return baseResult
        
        # Get resource type name in shortname for table name reference
        rtype = baseResult[0]["__rtype__"]
        tyShortName = rtype.split(":")[1]
        
        # Retrieve data from target resource type table
        query = "SELECT row_to_json({}) FROM {} WHERE resource_index = {}".format(tyShortName, tyShortName, baseResult[0]["index"])
        resourceResult = self._execQuery(query)
        
        result = []
        # If query result is not empty, append to result. TODO: if it empty, somehow it is inconsistent. Maybe delete it from resource table.
        if len(resourceResult) > 0:
            # merge data from resources table and specific resource type table
            result.append( baseResult[0] | resourceResult[0] )
        
        return result
    
    
    def _selectByACPI(self, acpi: str) -> list[dict]:
        """ Retrieve all resource that contain ACP in the acpi attributes

        Args:
            acpi (str): ACP resourceId that want to search for in resource acpi attribute

        Returns:
            list[dict]: list of resources (any resource type)
        """        
        query = f"""
                    SELECT row_to_json(results) FROM (
                        SELECT * FROM resources WHERE acpi @> '[\"{acpi}\"]'
                    ) as results;
                    """
        baseResult = self._execQuery(query)
        
        # TODO: Optimize query call by using pgsql loop in the query?
        result = []
        for base in baseResult:
            # Get resource type name in shortname for table name reference
            rtype = base["__rtype__"]
            tyShortName = rtype.split(":")[1]
            
            # Retrieve data from target resource type table
            query = "SELECT row_to_json({}) FROM {} WHERE resource_index = {}".format(tyShortName, tyShortName, base["index"])
            resourceResult = self._execQuery(query)
            # If query result is not empty, append to result. TODO: if it empty, somehow it is inconsistent. Maybe delete it from resource table.
            if len(resourceResult) > 0:
                result.append( base | resourceResult[0] )
        
        return result
    
    
    def _selectByMID(self, mid: str) -> list[dict]:
        """ Retrieve every group resource that contains ri in the mid attribute. 

        Args:
            mid (str): resourceId of member that want to search for in mid attribute of group

        Returns:
            list[dict]: list of group resource
        """        
        
        query = f"""
                SELECT row_to_json(results) FROM (
                    SELECT * FROM resources, grp WHERE grp.mid @> '[\"{mid}\"]' AND grp.resource_index = resources.index
                ) as results;
                """
        return self._execQuery(query)
    
    
    def _selectByMCSI(self, mcsi: str, filter:Optional[Callable[[JSON], bool]] = None) -> list[dict]:
        """ Retrieve every resource that match mcsi at it's 'at' attribute, and use filter param if provided

        Args:
            mcsi (str): csi pattern that want to look for in 'at' attribute
            filter (Optional[Callable[[JSON], bool]], optional): Other filter. Defaults to None.

        Returns:
            list[dict]: List of resource in dict that in filter criteria
        """        
        # Retrieve only needed attribute that match mcsi condition
        query = f"""
                SELECT row_to_json(results) FROM (
                    SELECT ri, at, ty, __announcedto__ FROM resources, jsonb_array_elements_text(at)
                    WHERE VALUE ILIKE '{mcsi}%'
                ) as results;
                """
        filterResult = self._execQuery(query)
        
        # Sanity check result
        if len(filterResult) == 0:
            return []
        
        result = []
        
        # If filter not provided, then retrieve all attributes for each resource found before
        if not filter:
            for res in filterResult:
                if (l := len(val := self._selectByRI(res["ri"], res["ty"]))) > 0:
                    result.append(val[0])
                else:
                    L.logWarn(f'Cannot retrieve {res["ri"]} for {res["ty"]}')
                    
            return result
                    
        # Do filter
        for res in filterResult:
            if not filter(res):
                continue
            
            # If filter return True, then retrieve all attributes for 'that' resource
            if (l := len(val := self._selectByRI(res["ri"], res["ty"]))) > 0:
                result.append(val[0])
            else:
                L.logWarn(f'Cannot retrieve {res["ri"]} for {res["ty"]}')
        
        return result
    

    def _execQuery(self, query: str) -> list:
        # TODO: Remove newline from query string
        L.isDebug and L.logDebug(f"Query: {query}")
        result = []
        with self._lockExecution:
            try:
                with self._connection, self._connection.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    for row in rows:
                        result.append(row[0])
            except Exception as e:
                L.logErr('Failed exec query: {}'.format(str(e)))
                # L.isDebug and L.logDebug("Rollback connection")
            
        return result
    
    
    def _execManipulationQuery(self, query: str) -> bool:
        L.isDebug and L.logDebug(f'Query: {query}')
        success = True
        with self._lockExecution:
            try:
                with self._connection, self._connection.cursor() as cursor:
                    cursor.execute(query)
            except Exception as e:
                L.isInfo and L.logErr('Failed exec query: {}'.format(str(e)))
                success = False

        return success
            

if __name__ == "__main__":
    binding = PostgresBinding()
    # print( binding.retrieveOldestResource(3) )
    # print( binding.searchResources(ty=5) )
    # print( binding.searchResources(pi = "cse1234", ty=1) )
    # print( binding.retrieveLatestResource(ty=1,pi="cse1234") )
    
    # print( binding.retrieveResourceAttribute(acpi="acp1234") )
    
    # print( binding.retrieveExpiredResource() )
    
    print( binding.countResources((1,)) )
    
    binding.closeConnection()
    
    