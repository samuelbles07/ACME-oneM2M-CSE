#
#	Storage.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	Store, retrieve and manage resources in the database. It currently relies on
#	the document database TinyDB. It is possible to store resources either on disc
#	or just in memory.
#

"""	This module defines storage managers and drivers for database access.
"""

from __future__ import annotations
from typing import Callable, cast, List, Optional, Tuple

import os, shutil
from threading import Lock
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage
from tinydb.table import Document
from tinydb.operations import delete 

from ..etc.Types import ResourceTypes, Result, ResponseStatusCode, JSON
from ..etc import DateUtils
from ..services.Configuration import Configuration
from ..services import CSE
from ..resources.Resource import Resource
from ..resources import Factory
from ..services.Logging import Logging as L
from ..services.StoragePostgres import PostgresBinding


class Storage(object):
	"""	This class implements the entry points to the CSE's underlying database functions.

		Attributes:
			inMemory: Indicator whether the database is located in memory (volatile) or on disk.
			dbPath: In case *inMemory* is "False" this attribute contains the path to a directory where the database is stored in disk.
			dbReset: Indicator that the database should be reset or cleared during start-up.
	"""

	def __init__(self) -> None:
		"""	Initialization of the storage manager.
		"""
		# Reset dbs?
		if Configuration.get('db.resetOnStartup'):
			self._backupDB()	# In this case do a backup *before* startup.
			self.purge()

		# Initiate postgres binding
		self._postgres = PostgresBinding()
		L.isInfo and L.log('Storage initialized')


	def shutdown(self) -> bool:
		"""	Shutdown the storage manager.
		
			Return:
				Always True.
		"""
		# Close postgres connnection
		self._postgres.closeConnection()
		L.isInfo and L.log('Storage shut down')
		return True


	def purge(self) -> None:
		"""	Reset and clear the databases.
		"""
		# TODO: Do purge DB here
		pass
	

	def _backupDB(self) -> bool:
		"""	Creating a backup from the DB to a sub directory.

			Return:
				Boolean indicating the success of the backup operation.
		"""
		# TODO: Do backup here. Is it needed?
		return True
		

	#########################################################################
	##
	##	Resources
	##


	def createResource(self, resource:Resource, overwrite:Optional[bool] = True) -> Result:
		"""	Create a new resource in the database.
		
			Args:
				resource: The resource to store in the database.
				overwrite: Indicator whether an existing resource shall be overwritten.
			
			Return:
				Result object indicating success or error status.
		"""
		ri  = resource.ri
		srn = resource.getSrn()
		# L.logDebug(f'Adding resource (ty: {resource.ty}, ri: {resource.ri}, rn: {resource.rn}, srn: {srn}')
		if overwrite:
			L.isDebug and L.logDebug('Resource enforced overwrite')
			# self.db.upsertResource(resource)
			self._postgres.upsertResource(resource)
		else: 
			if not self.hasResource(ri, srn):	# Only when not resource does not exist yet
				if not self._postgres.insertResource(resource):
					return Result(status = False, rsc = ResponseStatusCode.UNKNOWN)
			else:
				return Result.errorResult(rsc = ResponseStatusCode.conflict, dbg = L.logWarn(f'Resource already exists (Skipping): {resource} ri: {ri} srn:{srn}'))

		return Result(status = True, rsc = ResponseStatusCode.created)


	def hasResource(self, ri:Optional[str] = None, srn:Optional[str] = None) -> bool:
		"""	Check whether a resource with either the ri or the srn already exists.

			Either one of *ri* or *srn* must be provided.

			Args:
				ri: Optional resource ID.
				srn: Optional structured resource name.
			Returns:
				True when a resource with the ID or structured resource name exists.
		"""
		return (ri is not None and self._postgres.hasResource(ri = ri)) or (srn is not None and self._postgres.hasResource(srn = srn))


	def retrieveResource(self,	ri:Optional[str] = None, 
								csi:Optional[str] = None,
								srn:Optional[str] = None, 
								aei:Optional[str] = None) -> Result:
		""" Return a resource via different addressing methods. 

			Either one of *ri*, *srn*, *csi*, or *aei* must be provided.

			Args:
				ri:  The resource is retrieved via its rersource ID.
				csi: The resource is retrieved via its CSE-ID.
				srn: The resource is retrieved via its structured resource name.
				aei: The resource is retrieved via its AE-ID.
			Returns:
				The resource is returned in a `Result` object.
		"""
		resources = []

		if ri:		# get a resource by its ri
			# L.logDebug(f'Retrieving resource ri: {ri}')
			resources = self._postgres.searchResources(ri = ri)

		elif srn:	# get a resource by its structured rn
			# L.logDebug(f'Retrieving resource srn: {srn}')
			# get the ri via the srn from the identifers table
			resources = self._postgres.searchResources(srn = srn)

		elif csi:	# get the CSE by its csi
			# L.logDebug(f'Retrieving resource csi: {csi}')
			resources = self._postgres.searchResources(csi = csi)
		
		elif aei:	# get an AE by its AE-ID
			resources = self._postgres.searchResources(aei = aei)

		# L.logDebug(resources)
		# return CSE.dispatcher.resourceFromDict(resources[0]) if len(resources) == 1 else None,
		if (l := len(resources)) == 1:
			return Factory.resourceFromDict(resources[0])
		elif l == 0:
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = 'resource not found')

		return Result.errorResult(rsc = ResponseStatusCode.internalServerError, dbg = 'database inconsistency')


	def retrieveResourceRaw(self, ri:str) -> Result:
		"""	Retrieve a resource as a raw dictionary.

			Args:
				ri:  The resource is retrieved via its rersource ID.
			Returns:
				The resource dictionary is returned in a Result object in the *resource* attribute.
		"""
		resources = self._postgres.searchResources(ri = ri)
		if (l := len(resources)) == 1:
			return Result(status = True, resource = resources[0])
		elif l == 0:
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = 'resource not found')
		return Result.errorResult(rsc = ResponseStatusCode.internalServerError, dbg = 'database inconsistency')


	def retrieveResourcesByType(self, ty:ResourceTypes) -> list[Document]:
		""" Return all resources of a certain type. 

			Args:
				ty: resource type to retrieve.
			Returns:
				List of resource `Document`. 
		"""
		# L.logDebug(f'Retrieving all resources ty: {ty}')
		return self._postgres.searchResources(ty = int(ty))


	def retrieveResourceBy(self, acpi: Optional[str] = None,
							mid: Optional[str] = None,
							ty: Optional[int] = None,
							mcsi: Optional[str] = None,
							filter:Optional[Callable[[JSON], bool]] = None) -> list[Resource]:
		""" Retrieve list of resource based on attribute value 

		Args:
			acpi (Optional[str], optional): If provided, search resource that use ACP ri in it's resource acpi attribute. Defaults to None.
			mid (Optional[str], optional): If provided, Retrieve every group resource that contains ri in the mid attribute. Defaults to None.
			ty (Optional[int], optional): Haven't used. Defaults to None.
			mcsi (Optional[str], optional): If provided, Retrieve every resource that match mcsi at it's 'at' attribute, and use filter param if provided. Defaults to None.
			filter (Optional[Callable[[JSON], bool]], optional): If provided, mcsi need to provided too. Defaults to None.

		Returns:
			list[Resource]: list of resource that match attribute value
		"""
		return  [ res	for each in self._postgres.retrieveResourceByAttribute(acpi = acpi, mid = mid, ty = ty, mcsi = mcsi, filter = filter)
						if (res := Factory.resourceFromDict(each).resource)
				]


	def retrieveOldestResource(self, ty: int, pi:Optional[str] = None) -> Optional[JSON]:
		return self._postgres.retrieveOldestResource(ty=ty, pi = pi)


	def retrieveLatestResource(self, ty: int, pi:Optional[str] = None) -> Optional[JSON]:
		return self._postgres.retrieveLatestResource(ty = ty, pi = pi)

	def retrieveExpiredResource(self) -> list[Resource]:
		""" Retrieve list of resource that already expired

		Returns:
			list[Resource]: list of expired resource
		"""		
		return  [ res	for each in self._postgres.retrieveExpiredResource()
						if (res := Factory.resourceFromDict(each).resource)
				]


	def updateResource(self, resource:Resource) -> Result:
		"""	Update a resource in the database.

			Args:
				resource: Resource to update.
			Return:
				Result object.
		"""
		# L.logDebug(f'Updating resource (ty: {resource.ty}, ri: {resource.ri}, rn: {resource.rn})')
		if self._postgres.updateResource(resource):
			# remove nullified fields from resource
			for k in list(resource.dict):
				if resource.dict[k] is None:	# only remove the real None attributes, not those with 0
					del resource.dict[k]
			return Result(status = True, resource = resource, rsc = ResponseStatusCode.updated)
		else:
			return Result(status = False, resource = resource, rsc = ResponseStatusCode.UNKNOWN)


	def updateResourceBy(self, ri: str, data: JSON) -> Result:
     	# TODO: Update resource by not re insert everything
		pass


	def deleteResource(self, resource:Resource) -> Result:
		"""	Delete a resource from the database.

			Args:
				resource: Resource to delete.
			Return:
				Result object.
		"""
		# L.logDebug(f'Removing resource (ty: {resource.ty}, ri: {resource.ri}, rn: {resource.rn}')
		self._postgres.deleteResource(resource)
		return Result(status = True, rsc = ResponseStatusCode.deleted)


	def directChildResources(self, pi:str, 
								   ty:Optional[ResourceTypes] = None, 
								   raw:Optional[bool] = False) -> list[Document]|list[Resource]:
		"""	Return a list of direct child resources, or an empty list

			Args:
				pi: The parent resource's Resource ID.
				ty: Optional resource type to filter the result.
				raw: When "True" then return the child resources as resource dictionary instead of resources.
			Returns:
				Return a list of resources, or a list of raw resource dictionaries.
		"""
  
		docs = self._postgres.searchResources(pi = pi, ty = ( int(ty) if ty != None else ty))
		return docs if raw else cast(List[Resource], list(map(lambda x: Factory.resourceFromDict(x).resource, docs)))
		


	def countDirectChildResources(self, pi:str, ty:Optional[ResourceTypes] = None) -> int:
		"""	Count the number of direct child resources.

			Args:
				pi: The parent resource's Resource ID.
				ty: Optional resource type to filter the result.
			Returns:
				The number of child resources.
		"""

		return self._postgres.countResourcesBy(pi = pi, ty = int(ty) if ty is not None else None )


	def countResources(self, ty: Tuple[ResourceTypes, ...] = None) -> int:
		"""	Count the overall number of CSE resources.

			Returns:
				The number of CSE resources.
		"""
		if not isinstance(ty, tuple) and ty != None:
			L.logErr("Type of argument passed is not a tuple or None as required")
			return 0

		return self._postgres.countResources(ty)


	def identifier(self, ri:str) -> list[JSON]:
		"""	Search for the resource identifer mapping with the given unstructured resource ID.

			Args:
				ri: Unstructured resource ID for the mapping to look for.
			Return:
				List of found resources identifier mappings, or an empty list.
		"""
		return self._postgres.searchIdentifiers(ri = ri)


	def structuredIdentifier(self, srn:str) -> list[JSON]:
		"""	Search for the resource identifer mapping with the given structured resource ID.

			Args:
				srn: Structured resource ID for the mapping to look for.
			Return:
				List of found resources identifier mappings, or an empty list.
		"""
		return self._postgres.searchIdentifiers(srn = srn)


	#########################################################################
	##
	##	Subscriptions
	##

	def getSubscription(self, ri:str) -> Optional[JSON]:
		# TODO: This here can retrieve only specific attribute as legacy does
		# L.logDebug(f'Retrieving subscription: {ri}')
		result = self._postgres.searchResources(ri = ri, ty = int(ResourceTypes.SUB))
		if len(result) > 0:
			# Add enc field member to it's own field
			enc = result[0]["enc"]
			if enc: # Sanity check if enc is None
				result[0]["net"] = enc.get("net") # type: dict
				result[0]["atr"] = enc.get("atr") # type: dict
				result[0]["chty"] = enc.get("chty") # type: dict
			else:
				result[0]["net"] = None
				result[0]["atr"] = None
				result[0]["chty"] = None    
			# enc field is not neccessary anymore
			# del result[0]["enc"]
			# Somehow in legacy TinyDB binding storate (sub table), 'nu' field name is changed to 'nus'
			result[0]["nus"] = result[0].pop("nu")
			return result[0]
   
		return None


	def getSubscriptionsForParent(self, pi:str) -> list[JSON]:
		# L.logDebug(f'Retrieving subscriptions for parent: {pi}')
		# return self.db.searchSubscriptions(pi = pi)
		result = self._postgres.searchResources(pi = pi, ty = int(ResourceTypes.SUB))
		for idx, _ in enumerate(result):
			# Add enc field member to it's own field
			enc = result[idx]["enc"]
			if enc: # Sanity check if enc is None
				result[idx]["net"] = enc.get("net") # type: dict
				result[idx]["atr"] = enc.get("atr") # type: dict
				result[idx]["chty"] = enc.get("chty") # type: dict
			else:
				result[idx]["net"] = None
				result[idx]["atr"] = None
				result[idx]["chty"] = None
			# enc field is not neccessary anymore
			# del result[idx]["enc"]
			# Somehow in legacy TinyDB binding storate (sub table), 'nu' field name is changed to 'nus'
			result[idx]["nus"] = result[idx].pop("nu")
   
		return result


	#########################################################################
	##
	##	BatchNotifications
	##

	def addBatchNotification(self, ri:str, nu:str, request:JSON) -> bool:
		return self._postgres.addBatchNotification(ri, nu, request)


	def countBatchNotifications(self, ri:str, nu:str) -> int:
		return self._postgres.countBatchNotifications(ri, nu)


	def getBatchNotifications(self, ri:str, nu:str) -> list[Document]:
		return self._postgres.getBatchNotifications(ri, nu)


	def removeBatchNotifications(self, ri:str, nu:str) -> bool:
		return self._postgres.removeBatchNotifications(ri, nu)



	#########################################################################
	##
	##	Statistics
	##

	def getStatistics(self) -> JSON:
		"""	Retrieve the statistics data from the DB.
		"""
		return {}
		# return self.db.searchStatistics()


	def updateStatistics(self, stats:JSON) -> bool:
		"""	Update the statistics DB with new data.
		"""
		return True
		# return self.db.upsertStatistics(stats)


	def purgeStatistics(self) -> None:
		"""	Purge the statistics DB.
		"""
		pass
		# self.db.purgeStatistics()


