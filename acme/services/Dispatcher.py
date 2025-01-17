#
#	Dispatcher.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	Most internal requests are routed through here.
#

from __future__ import annotations
from typing import List, Tuple, cast, Sequence, Optional

import operator
import sys
from copy import deepcopy

from ..helpers import TextTools
from ..etc.Types import FilterCriteria, FilterUsage, Operation, ResourceTypes
from ..etc.Types import FilterOperation
from ..etc.Types import Permission
from ..etc.Types import DesiredIdentifierResultType
from ..etc.Types import ResultContentType
from ..etc.Types import ResponseStatusCode
from ..etc.Types import Result
from ..etc.Types import CSERequest
from ..etc.Types import JSON
from ..etc import Utils
from ..etc import DateUtils
from ..services import CSE
from ..services.Configuration import Configuration
from ..resources import Factory as Factory
from ..resources.Resource import Resource
from ..resources.SMD import SMD
from ..services.Logging import Logging as L


# TODO NOTIFY optimize local resource notifications

class Dispatcher(object):

	def __init__(self) -> None:
		self.csiSlashLen 				= len(CSE.cseCsiSlash)
		self.sortDiscoveryResources 	= Configuration.get('cse.sortDiscoveredResources')
		L.isInfo and L.log('Dispatcher initialized')


	def shutdown(self) -> bool:
		"""	Shutdown the Dispatcher servide.
			
			Return:
				Boolean indicating the success.
		"""
		L.isInfo and L.log('Dispatcher shut down')
		return True



	# The "xxxRequest" methods handle http requests while the "xxxResource"
	# methods handle actions on the resources. Security/permission checking
	# is done for requests, not on resource actions.


	#########################################################################

	#
	#	Retrieve resources
	#

	def processRetrieveRequest(self, request:CSERequest, 
									 originator:str, 
									 id:Optional[str] = None) -> Result:
		"""	Process a RETRIEVE request. Retrieve and discover resource(s).

			Args:
				request: The incoming request.
				originator: The requests originator.
				id: Optional ID of the request.
			Return:
				Result object.
		"""
		L.isDebug and L.logDebug(f'Process RETRIEVE request for id: {request.id}|{request.srn}')

		# handle transit requests first
		if Utils.localResourceID(request.id) is None and Utils.localResourceID(request.srn) is None:
			return CSE.request.handleTransitRetrieveRequest(request)

		srn, id = self._checkHybridID(request, id) # overwrite id if another is given

		# Handle operation execution time and check request expiration
		self._handleOperationExecutionTime(request)
		if not (res := self._checkRequestExpiration(request)).status:
			return res

		# handle fanout point requests
		if (fanoutPointResource := Utils.fanoutPointResource(srn)) and fanoutPointResource.ty == ResourceTypes.GRP_FOPT:
			L.isDebug and L.logDebug(f'Redirecting request to fanout point: {fanoutPointResource.getSrn()}')
			return fanoutPointResource.handleRetrieveRequest(request, srn, request.originator)

		# Handle PollingChannelURI RETRIEVE
		if (pollingChannelURIResource := Utils.pollingChannelURIResource(srn)):		# We need to check the srn here
			if not CSE.security.hasAccessToPollingChannel(originator, pollingChannelURIResource):
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = L.logDebug(f'Originator: {originator} has not access to <pollingChannelURI>: {id}'))
			L.isDebug and L.logDebug(f'Redirecting request <PCU>: {pollingChannelURIResource.getSrn()}')
			return pollingChannelURIResource.handleRetrieveRequest(request, id, originator)


		# EXPERIMENTAL
		# Handle latest and oldest RETRIEVE
		if (laOlResource := Utils.latestOldestResource(srn)):		# We need to check the srn here
			# Check for virtual resource
			if laOlResource.isVirtual(): 
				if not (res := laOlResource.handleRetrieveRequest(request = request, originator = originator)).status:
					return res
				if not CSE.security.hasAccess(originator, res.resource, Permission.RETRIEVE):
					return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = f'originator has no permission for {Permission.RETRIEVE}')
				return res

		# The permission also indicates whether this is RETRIEVE or DISCOVERY
		permission = Permission.DISCOVERY if request.fc.fu == FilterUsage.discoveryCriteria else Permission.RETRIEVE

		L.isDebug and L.logDebug(f'Discover/Retrieve resources (rcn: {request.rcn}, fu: {request.fc.fu.name}, drt: {request.drt.name}, fc: {str(request.fc)}, rcn: {request.rcn.name}, attributes: {str(request.fc.attributes)}, sqi: {request.sqi})')

		#
		#	Normal Retrieve
		# 	 Retrieve the target resource, because it is needed for some rcn (and the default)
		#

		# Check semantic discovery (sqi present and False)
		if request.sqi is not None and not request.sqi:
			# Get all accessible semanticDescriptors
			if not (res := self.discoverResources(id, originator, filterCriteria = FilterCriteria(ty = [ResourceTypes.SMD]))).status:
				return res
			L.isDebug and L.logDebug(f'Direct discovered SMD: {res.data}')

			# Execute semantic resource discovery
			if not (res := CSE.semantic.executeSemanticDiscoverySPARQLQuery(originator, request.fc.smf, cast(Sequence[SMD], res.data))).status:
				return res
			return Result(status = True, rsc = ResponseStatusCode.OK, resource = self._resourcesToURIList(cast(List[Resource], res.data), request.drt))

		else:
			if request.rcn in [ ResultContentType.attributes, 
								ResultContentType.attributesAndChildResources, 
								ResultContentType.childResources, 
								ResultContentType.attributesAndChildResourceReferences, 
								ResultContentType.originalResource ]:
				if not (res := self.retrieveResource(id, originator, request)).status:
					return res # error
				if not CSE.security.hasAccess(originator, res.resource, permission):
					return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = f'originator has no permission for {permission}')

				# if rcn == attributes then we can return here, whatever the result is
				if request.rcn == ResultContentType.attributes:
					if not (resCheck := res.resource.willBeRetrieved(originator, request)).status:	# resource instance may be changed in this call
						return resCheck
					return res

				resource = cast(Resource, res.resource)	# root resource for the retrieval/discovery

				# if rcn == original-resource we retrieve the linked resource
				if request.rcn == ResultContentType.originalResource:
					# Some checks for resource validity
					if not resource.isAnnounced():
						return Result.errorResult(dbg = L.logDebug(f'Resource {resource.ri} is not an announced resource'))
					if not (lnk := resource.lnk):	# no link attribute?
						L.logErr('Internal Error: missing lnk attribute in target resource')
						return Result.errorResult(rsc = ResponseStatusCode.internalServerError, dbg = 'missing lnk attribute in target resource')

					# Retrieve and check the linked-to request
					if (res := self.retrieveResource(lnk, originator, request)).resource:
						if not (resCheck := res.resource.willBeRetrieved(originator, request)).status:	# resource instance may be changed in this call
							return resCheck
					return res
			
			#
			#	Semantic query request
			#	This is indicated by rcn = semantic content
			#
			if request.rcn == ResultContentType.semanticContent:
				L.isDebug and L.logDebug('Performing semantic discovery / query')
				# Validate SPARQL in semanticFilter
				if not (res := CSE.semantic.validateSPARQL(request.fc.smf)).status:
					return res

				# Get all accessible semanticDescriptors
				if not (res := self.discoverResources(id, originator, filterCriteria = FilterCriteria(ty = [ResourceTypes.SMD]))).status:
					return res
				
				# Execute semantic query
				if not (res := CSE.semantic.executeSPARQLQuery(request.fc.smf, cast(Sequence[SMD], res.data))).status:
					return res

				L.isDebug and L.logDebug(f'SPARQL query result: {res.data}')
				return Result(status = True, rsc = ResponseStatusCode.OK, data = { 'm2m:qres' : res.data })

		#
		#	Discovery request
		#
		if not (res := self.discoverResources(id, originator, request.fc, permission = permission)).status:	# not found?
			return res.errorResultCopy()				

		# check and filter by ACP. After this allowedResources only contains the resources that are allowed
		allowedResources = []
		for r in cast(List[Resource], res.data):
			if CSE.security.hasAccess(originator, r, permission):
				if not r.willBeRetrieved(originator, request).status:	# resource instance may be changed in this call
					continue
				allowedResources.append(r)


		#
		#	Handle more sophisticated RCN
		#

		if request.rcn == ResultContentType.attributesAndChildResources:
			self.resourceTreeDict(allowedResources, resource)	# the function call add attributes to the target resource
			return Result(status = True, rsc = ResponseStatusCode.OK, resource = resource)

		elif request.rcn == ResultContentType.attributesAndChildResourceReferences:
			self._resourceTreeReferences(allowedResources, resource, request.drt, 'ch')	# the function call add attributes to the target resource
			return Result(status = True, rsc = ResponseStatusCode.OK, resource = resource)

		elif request.rcn == ResultContentType.childResourceReferences: 
			#childResourcesRef:JSON = { resource.tpe: {} }  # Root resource with no attribute
			#childResourcesRef = self._resourceTreeReferences(allowedResources,  None, request.drt, 'm2m:rrl')
			# self._resourceTreeReferences(allowedResources, childResourcesRef[resource.tpe], request.drt, 'm2m:rrl')
			childResourcesRef = self._resourceTreeReferences(allowedResources, None, request.drt, 'm2m:rrl')
			return Result(status = True, rsc = ResponseStatusCode.OK, resource = childResourcesRef)

		elif request.rcn == ResultContentType.childResources:
			childResources:JSON = { resource.tpe : {} } #  Root resource as a dict with no attribute
			self.resourceTreeDict(allowedResources, childResources[resource.tpe]) # Adding just child resources
			return Result(status = True, rsc = ResponseStatusCode.OK, resource = childResources)

		elif request.rcn == ResultContentType.discoveryResultReferences: # URIList
			return Result(status = True, rsc = ResponseStatusCode.OK, resource = self._resourcesToURIList(allowedResources, request.drt))

		else:
			return Result.errorResult(dbg = 'unsuppored rcn for RETRIEVE')


	def retrieveResource(self, id:str, 
							   originator:Optional[str] = None, 
							   request:Optional[CSERequest] = None, 
							   postRetrieveHook:Optional[bool] = False) -> Result:
		"""	Retrieve a resource locally or from remote CSE.

			Args:
				id:	If the *id* is in SP-relative format then first check whether this is for the local CSE.
					If yes, then adjust the ID and try to retrieve it.
					If no, then try to retrieve the resource from a connected (!) remote CSE.
				originator:	The originator of the request.
				postRetrieveHook: Only when retrieving localls, invoke the Resource's *willBeRetrieved()* callback.
			Return:
				Result instance.

		"""
		if id:
			if id.startswith(CSE.cseCsiSlash) and len(id) > self.csiSlashLen:		# TODO for all operations?
				id = id[self.csiSlashLen:]
			else:
				# Retrieve from remote
				if Utils.isSPRelative(id):
					return CSE.remote.retrieveRemoteResource(id, originator)

		# Retrieve locally
		if Utils.isStructured(id):
			res = self.retrieveLocalResource(srn = id, originator = originator, request = request) 
		else:
			res = self.retrieveLocalResource(ri = id, originator = originator, request = request)
		if res.status and postRetrieveHook:
			res.resource.willBeRetrieved(originator, request, subCheck = False)
		return res




	def retrieveLocalResource(self, ri:Optional[str] = None, 
									srn:Optional[str] = None, 
									originator:Optional[str] = None, 
									request:Optional[CSERequest] = None) -> Result:
		L.isDebug and L.logDebug(f'Retrieve local resource: {ri}|{srn} for originator: {originator}')

		if ri:
			result = CSE.storage.retrieveResource(ri = ri)		# retrieve via normal ID
		elif srn:
			result = CSE.storage.retrieveResource(srn = srn) 	# retrieve via srn. Try to retrieve by srn (cases of ACPs created for AE and CSR by default)
		else:
			result = Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = 'resource not found')

		# EXPERIMENTAL remove this
		# if resource := cast(Resource, result.resource):	# Resource found
		# 	# Check for virtual resource
		# 	if resource.ty not in [T.GRP_FOPT, T.PCH_PCU] and resource.isVirtual(): # fopt, PCU are handled elsewhere
		# 		return resource.handleRetrieveRequest(request=request, originator=originator)	# type: ignore[no-any-return]
		# 	return result
		# # error
		# L.isDebug and L.logDebug(f'{result.dbg}: ri:{ri} srn:{srn}')

		return result


	#########################################################################
	#
	#	Discover Resources
	#

	def discoverResources(self,
						  id:str,
						  originator:str, 
						  filterCriteria:Optional[FilterCriteria] = None,
						  rootResource:Optional[Resource] = None, 
						  permission:Optional[Permission] = Permission.DISCOVERY) -> Result:
		L.isDebug and L.logDebug('Discovering resources')

		if not rootResource:
			if not (res := self.retrieveResource(id)).resource:
				return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = res.dbg)
			rootResource = res.resource
		
		if not filterCriteria:
			filterCriteria = FilterCriteria()

		# Apply defaults. This is not done in the FilterCriteria class bc there we only store he provided values
		lvl:int = filterCriteria.lvl if filterCriteria.lvl is not None else sys.maxsize
		fo:FilterOperation = filterCriteria.fo if filterCriteria.fo is not None else FilterOperation.AND
		ofst:int = filterCriteria.ofst if filterCriteria.ofst is not None else 1
		lim:int = filterCriteria.lim if filterCriteria.lim is not None else sys.maxsize

		# get all direct children and slice the page (offset and limit)
		dcrs = self.directChildResources(id)[ofst-1:ofst-1 + lim]	# now dcrs only contains the desired child resources for ofst and lim

		# a bit of optimization. This length stays the same.
		allLen = len(filterCriteria.attributes) if filterCriteria.attributes else 0
		if (criteriaAttributes := filterCriteria.criteriaAttributes()):
			allLen += ( len(criteriaAttributes) +
			  (len(_v)-1 if (_v := criteriaAttributes.get('ty'))  is not None else 0) +		# -1 : compensate for len(conditions) in line 1
			  (len(_v)-1 if (_v := criteriaAttributes.get('cty')) is not None else 0) +		# -1 : compensate for len(conditions) in line 1 
			  (len(_v)-1 if (_v := criteriaAttributes.get('lbl')) is not None else 0) 		# -1 : compensate for len(conditions) in line 1 
			)

		# Discover the resources
		discoveredResources = self._discoverResources(rootResource, 
													  originator, 
													  level = lvl, 
													  fo = fo, 
													  allLen = allLen, 
													  dcrs = dcrs, 
													  filterCriteria = filterCriteria,
													  permission=permission)

		# NOTE: this list contains all results in the order they could be found while
		#		walking the resource tree.
		#		DON'T CHANGE THE ORDER. DON'T SORT.
		#		Because otherwise the tree cannot be correctly re-constructed otherwise

		# Apply ARP if provided
		if filterCriteria.arp:
			_resources = []
			for resource in discoveredResources:
				# Check existence and permissions for the .../{arp} resource
				srn = f'{resource.getSrn()}/{filterCriteria.arp}'
				if (res := self.retrieveResource(srn)).resource and CSE.security.hasAccess(originator, res.resource, permission):
					_resources.append(res.resource)
			discoveredResources = _resources	# re-assign the new resources to discoveredResources

		return Result(status = True, data = discoveredResources)


	def _discoverResources(self, rootResource:Resource,
								 originator:str, 
								 level:int, 
								 fo:int, 
								 allLen:int, 
								 dcrs:Optional[list[Resource]] = None, 
								 filterCriteria:Optional[FilterCriteria] = None,
								 permission:Optional[Permission] = Permission.DISCOVERY) -> list[Resource]:
		if not rootResource or level == 0:		# no resource or level == 0
			return []

		# get all direct children, if not provided
		if not dcrs:
			if len(dcrs := self.directChildResources(rootResource.ri)) == 0:
				return []

		# Filter and add those left to the result
		discoveredResources = []
		for resource in dcrs:

			# Exclude virtual resources
			if resource.isVirtual():
				continue

			# check permissions and filter. Only then add a resource
			# First match then access. bc if no match then we don't need to check permissions (with all the overhead)
			if self._matchResource(resource, 
								   fo, 
								   allLen, 
								   filterCriteria) and CSE.security.hasAccess(originator, resource, permission):
				discoveredResources.append(resource)

			# Iterate recursively over all (not only the filtered!) direct child resources
			discoveredResources.extend(self._discoverResources(resource, 
															   originator, 
															   level-1, 
															   fo, 
															   allLen, 
															   filterCriteria = filterCriteria,
															   permission = permission))

		return discoveredResources


	def _matchResource(self, r:Resource, fo:int, allLen:int, filterCriteria:FilterCriteria) -> bool:	
		""" Match a filter to a resource. """

		# TODO: Implement a couple of optimizations. Can we determine earlier that a match will fail?

		ty = r.ty

		# get the parent resource
		#
		#	TODO when determines how the parentAttribute is actually encoded
		#
		# pr = None
		# if (pi := r.get('pi')) is not None:
		# 	pr = storage.retrieveResource(ri=pi)

		# The matching works like this: go through all the conditions, compare them, and
		# increment 'found' when matching. For fo=AND found must equal all conditions.
		# For fo=OR found must be > 0.
		found = 0

		# check conditions
		if filterCriteria:

			# Types
			# Multiple occurences of ty is always OR'ed. Therefore we add the count of
			# ty's to found (to indicate that the whole set matches)
			if tys := filterCriteria.ty:
				found += len(tys) if ty in tys else 0	
			if ct := r.ct:
				found += 1 if (c_crb := filterCriteria.crb) and (ct < c_crb) else 0
				found += 1 if (c_cra := filterCriteria.cra) and (ct > c_cra) else 0
			if lt := r.lt:
				found += 1 if (c_ms := filterCriteria.ms) and (lt > c_ms) else 0
				found += 1 if (c_us := filterCriteria.us) and (lt < c_us) else 0
			if (st := r.st) is not None:	# st is an int
				found += 1 if (c_sts := filterCriteria.sts) is not None and (st > c_sts) else 0	# st is an int
				found += 1 if (c_stb := filterCriteria.stb) is not None and (st < c_stb) else 0
			if et := r.et:
				found += 1 if (c_exb := filterCriteria.exb) and (et < c_exb) else 0
				found += 1 if (c_exa := filterCriteria.exa) and (et > c_exa) else 0

			# Check labels similar to types
			resourceLbl = r.lbl
			if resourceLbl and (lbls := filterCriteria.lbl):
				for l in lbls:
					if l in resourceLbl:
						found += len(lbls)
						break

			if ResourceTypes.isInstanceResource(ty):	# special handling for instance resources
				if (cs := r.cs) is not None:	# cs is an int
					found += 1 if (sza := filterCriteria.sza) is not None and cs >= sza else 0	# sizes ares ints
					found += 1 if (szb := filterCriteria.szb) is not None and cs < szb else 0

			# ContentFormats
			# Multiple occurences of cnf is always OR'ed. Therefore we add the count of
			# cnf's to found (to indicate that the whole set matches)
			# Similar to types.
			if ty in [ ResourceTypes.CIN ]:	# special handling for CIN
				if filterCriteria.cty:
					found += len(filterCriteria.cty) if r.cnf in filterCriteria.cty else 0

		# TODO childLabels
		# TODO parentLabels
		# TODO childResourceType
		# TODO parentResourceType


		# Attributes:
		for name, value in filterCriteria.attributes.items():
			if isinstance(value, str) and '*' in value:
				found += 1 if (rval := r[name]) is not None and TextTools.simpleMatch(str(rval), value) else 0
			else:
				found += 1 if (rval := r[name]) is not None and str(value) == str(rval) else 0

		# TODO childAttribute
		# TODO parentAttribute


		# L.isDebug and L.logDebug(f'fo: {fo}, found: {found}, allLen: {allLen}')
		# Test whether the OR or AND criteria is fullfilled
		if not ((fo == FilterOperation.OR  and found > 0) or 		# OR and found something
				(fo == FilterOperation.AND and allLen == found)		# AND and found everything
			   ): 
			return False

		return True


	#########################################################################
	#
	#	Add resources
	#

	def processCreateRequest(self, request:CSERequest, 
								   originator:str, 
								   id:Optional[str] = None) -> Result:
		"""	Process a CREATE request. Create and register resource(s).

			Args:
				request: The incoming request.
				originator: The requests originator.
				id: Optional ID of the request.
			Return:
				Result object.
		"""
		L.isDebug and L.logDebug(f'Process CREATE request for id: {request.id}|{request.srn}')

		# handle transit requests first
		if Utils.localResourceID(request.id) is None and Utils.localResourceID(request.srn) is None:
			return CSE.request.handleTransitCreateRequest(request)

		srn, id = self._checkHybridID(request, id) # overwrite id if another is given
		if not id and not srn:
			# if not (id := request.id):
			# 	return Result.errorResult(rsc = RC.notFound, dbg = L.logDebug('resource not found'))
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = L.logDebug('resource not found'))

		# Handle operation execution time and check request expiration
		self._handleOperationExecutionTime(request)
		if not (res := self._checkRequestExpiration(request)).status:
			return res

		# handle fanout point requests
		if (fanoutPointResource := Utils.fanoutPointResource(srn)) and fanoutPointResource.ty == ResourceTypes.GRP_FOPT:
			L.isDebug and L.logDebug(f'Redirecting request to fanout point: {fanoutPointResource.getSrn()}')
			return fanoutPointResource.handleCreateRequest(request, srn, request.originator)

		if (ty := request.ty) is None:	# Check for type parameter in request, integer
			return Result.errorResult(dbg = L.logDebug('type parameter missing in CREATE request'))

		# Some Resources are not allowed to be created in a request, return immediately
		if not ResourceTypes.isRequestCreatable(ty):
			return Result.errorResult(rsc = ResponseStatusCode.operationNotAllowed, dbg = f'CREATE not allowed for type: {ty}')

		# Get parent resource and check permissions
		L.isDebug and L.logDebug(f'Get parent resource and check permissions: {id}')
		if not (res := CSE.dispatcher.retrieveResource(id)).resource:
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = L.logWarn(f'Parent/target resource: {id} not found'))
		parentResource = cast(Resource, res.resource)

		if CSE.security.hasAccess(originator, parentResource, Permission.CREATE, ty = ty, parentResource = parentResource) == False:
			if ty == ResourceTypes.AE:
				return Result.errorResult(rsc = ResponseStatusCode.securityAssociationRequired, dbg = 'security association required')
			else:
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = 'originator has no privileges for CREATE')

		# Check for virtual resource
		if parentResource.isVirtual():
			return parentResource.handleCreateRequest(request, id, originator)	# type: ignore[no-any-return]

		# Create resource from the dictionary
		if not (nres := Factory.resourceFromDict(deepcopy(request.pc), pi = parentResource.ri, ty = ty)).status:	# something wrong, perhaps wrong type
			return nres
		newResource = nres.resource

		# Check whether the parent allows the adding
		if not (res := parentResource.childWillBeAdded(newResource, originator)).status:
			return res.errorResultCopy()

		# Check resource creation
		if not (rres := CSE.registration.checkResourceCreation(newResource, originator, parentResource)).status:
			return rres.errorResultCopy()

		# check whether the resource already exists, either via ri or srn
		# hasResource() may actually perform the test in one call, but we want to give a distinguished debug message
		if CSE.storage.hasResource(ri = newResource.ri):
			return Result.errorResult(rsc = ResponseStatusCode.conflict, dbg = L.logWarn(f'Resource with ri: {newResource.ri} already exists'))
		if CSE.storage.hasResource(srn = newResource.getSrn()):
			return Result.errorResult(rsc = ResponseStatusCode.conflict, dbg = L.logWarn(f'Resource with structured id: {newResource.getSrn()} already exists'))

		# originator might have changed during this check. Result.data contains this new originator
		originator = cast(str, rres.data) 					
		request.originator = originator	

		# Create the resource. If this fails we deregister everything
		if not (res := CSE.dispatcher.createLocalResource(newResource, parentResource, originator, request = request)).resource:
			CSE.registration.checkResourceDeletion(newResource) # deregister resource. Ignore result, we take this from the creation
			return res

		#
		# Handle RCN's
		#

		tpe = res.resource.tpe
		if request.rcn is None or request.rcn == ResultContentType.attributes:	# Just the resource & attributes, integer
			return res
		elif request.rcn == ResultContentType.modifiedAttributes:
			dictOrg = request.pc[tpe]
			dictNew = res.resource.asDict()[tpe]
			return Result(status = res.status, resource = { tpe : Utils.resourceModifiedAttributes(dictOrg, dictNew, request.pc[tpe]) }, rsc = res.rsc, dbg = res.dbg)
		elif request.rcn == ResultContentType.hierarchicalAddress:
			return Result(status = res.status, resource = { 'm2m:uri' : Utils.structuredPath(res.resource) }, rsc = res.rsc, dbg = res.dbg)
		elif request.rcn == ResultContentType.hierarchicalAddressAttributes:
			return Result(status = res.status, resource = { 'm2m:rce' : { Utils.noNamespace(tpe) : res.resource.asDict()[tpe], 'uri' : Utils.structuredPath(res.resource) }}, rsc = res.rsc, dbg = res.dbg)
		elif request.rcn == ResultContentType.nothing:
			return Result(status = res.status, rsc = res.rsc, dbg = res.dbg)
		else:
			return Result.errorResult(dbg = 'wrong rcn for CREATE')
		# TODO C.rcnDiscoveryResultReferences 


	def createResourceFromDict(self, dct:JSON, 
									 parentID:str, 
									 ty:ResourceTypes, 
									 originator:str) -> Result:
		# TODO doc
		# Create locally
		if (pID := Utils.localResourceID(parentID)) is not None:
			L.isDebug and L.logDebug(f'Creating local resource with ID: {pID} originator: {originator}')

			# Get the unstructured resource ID if necessary
			pID = Utils.riFromStructuredPath(pID) if Utils.isStructured(pID) else pID

			# Retrieve the parent resource
			if not (res := self.retrieveLocalResource(ri = pID, originator = originator)).status:
				L.isDebug and L.logDebug(f'Cannot retrieve parent resource: {pID}: {res.dbg}')
				return res
			parentResource = res.resource

			# Build a resource instance
			if not (res := Factory.resourceFromDict(dct, ty = ty, pi = pID)).status:
				return res

			# Check Permission
			if not CSE.security.hasAccess(originator, parentResource, Permission.CREATE, ty = ty, parentResource = parentResource):
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = L.logDebug(f'Originator: {originator} has no CREATE access to: {res.resource.ri}'))

			# Create it locally
			if not (res := self.createLocalResource(res.resource, parentResource = parentResource, originator = originator)).status:
				return res

			resRi = res.resource.ri
			resCsi = CSE.cseCsi
		
		# Create remotely
		else:
			L.isDebug and L.logDebug(f'Creating remote resource with ID: {pID} originator: {originator}')
			if not (res := CSE.request.sendCreateRequest((pri := Utils.toSPRelative(parentID)), 
														 originator = originator,
														 ty = ty,
														 content = dct)).status:
				return res
			
			# The request might have gone through normally and returned, but might still have failed on the remote CSE.
			# We need to set the status and the dbg attributes and return
			if res.rsc != ResponseStatusCode.created:
				res.status = False
				res.dbg = res.request.pc.get('dbg')
				return res

			resRi = Utils.findXPath(res.request.pc, '{*}/ri')
			resCsi = Utils.csiFromSPRelative(pri)
		
		# Return success and created resource and its (resouce ID, CSE-ID, parent ID)
		return Result(status = True, rsc = ResponseStatusCode.created, data = (resRi, resCsi, pID))


	def createLocalResource(self,
							resource:Resource,
							parentResource:Optional[Resource] = None,
							originator:Optional[str] = None,
							request:Optional[CSERequest] = None) -> Result:
		L.isDebug and L.logDebug(f'CREATING resource ri: {resource.ri}, type: {resource.ty}')

		if parentResource:
			L.isDebug and L.logDebug(f'Parent ri: {parentResource.ri}')
			if not parentResource.canHaveChild(resource):
				if resource.ty == ResourceTypes.SUB:
					return Result.errorResult(rsc = ResponseStatusCode.targetNotSubscribable, dbg = L.logWarn('Parent resource is not subscribable'))
				else:
					return Result.errorResult(rsc = ResponseStatusCode.invalidChildResourceType, dbg = L.logWarn(f'Invalid child resource type: {ResourceTypes(resource.ty).value}'))

		# if not already set: determine and add the srn
		if not resource.getSrn():
			resource.setSrn(Utils.structuredPath(resource))

		# add the resource to storage
		if not (res := resource.dbCreate(overwrite = False)).status:
			return res
		
		# Set release version to the resource, of available
		if request and request.rvi:
			resource.setRVI(request.rvi)

		# Activate the resource
		# This is done *after* writing it to the DB, because in activate the resource might create or access other
		# resources that will try to read the resource from the DB.
		if not (res := resource.activate(parentResource, originator)).status: 	# activate the new resource
			resource.dbDelete()
			return res.errorResultCopy()
		
		# Could be that we changed the resource in the activate, therefore write it again
		if not (res := resource.dbUpdate()).resource:
			resource.dbDelete()
			return res

		# send a create event
		CSE.event.createResource(resource)	# type: ignore


		if parentResource:
			parentResource = parentResource.dbReload().resource		# Read the resource again in case it was updated in the DB
			if not parentResource:
				self.deleteLocalResource(resource)
				return Result.errorResult(rsc = ResponseStatusCode.internalServerError, dbg = L.logWarn('Parent resource not found. Probably removed in between?'))
			parentResource.childAdded(resource, originator)			# notify the parent resource

			# Send event for parent resource
			CSE.event.createChildResource(parentResource)	# type: ignore

		return Result(status = True, resource = resource, rsc = ResponseStatusCode.created) 	# everything is fine. resource created.


	#########################################################################
	#
	#	Update resources
	#

	def processUpdateRequest(self, request:CSERequest, 
								   originator:str, 
								   id:Optional[str] = None) -> Result: 
		"""	Process a UPDATE request. Update resource(s).

			Args:
				request: The incoming request.
				originator: The requests originator.
				id: Optional ID of the request.
			Return:
				Result object.
		"""
		L.isDebug and L.logDebug(f'Process UPDATE request for id: {request.id}|{request.srn}')

		# handle transit requests first
		if Utils.localResourceID(request.id) is None and Utils.localResourceID(request.srn) is None:
			return CSE.request.handleTransitUpdateRequest(request)

		fopsrn, id = self._checkHybridID(request, id) # overwrite id if another is given

		# Unknown resource ?
		if not id and not fopsrn:
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = L.logDebug('resource not found'))

		# Handle operation execution time and check request expiration
		self._handleOperationExecutionTime(request)
		if not (res := self._checkRequestExpiration(request)).status:
			return res

		# handle fanout point requests
		if (fanoutPointResource := Utils.fanoutPointResource(fopsrn)) and fanoutPointResource.ty == ResourceTypes.GRP_FOPT:
			L.isDebug and L.logDebug(f'Redirecting request to fanout point: {fanoutPointResource.getSrn()}')
			return fanoutPointResource.handleUpdateRequest(request, fopsrn, request.originator)

		# Get resource to update
		if not (res := self.retrieveResource(id)).resource:
			L.isWarn and L.logWarn(f'Resource not found: {res.dbg}')
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = res.dbg)
		resource = cast(Resource, res.resource)
		if resource.readOnly:
			return Result.errorResult(rsc = ResponseStatusCode.operationNotAllowed, dbg = 'resource is read-only')

		# Some Resources are not allowed to be updated in a request, return immediately
		if ResourceTypes.isInstanceResource(resource.ty):
			return Result.errorResult(rsc = ResponseStatusCode.operationNotAllowed, dbg = f'UPDATE not allowed for type: {resource.ty}')

		#
		#	Permission check
		#	If this is an 'acpi' update?

		if not (res := CSE.security.hasAcpiUpdatePermission(request, resource, originator)).status:
			return res
		if not res.data:	# data == None or False indicates that this is NOT an ACPI update. In this case we need a normal permission check
			if CSE.security.hasAccess(originator, resource, Permission.UPDATE) == False:
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = 'originator has no privileges for UPDATE')

		# Check for virtual resource
		if resource.isVirtual():
			return resource.handleUpdateRequest(request, id, originator)	# type: ignore[no-any-return]

		dictOrg = deepcopy(resource.dict)	# Save for later


		if not (res := self.updateLocalResource(resource, deepcopy(request.pc), originator=originator)).resource:
			return res.errorResultCopy()
		resource = res.resource 	# re-assign resource (might have been changed during update)

		# Check resource update with registration
		if not (rres := CSE.registration.checkResourceUpdate(resource, deepcopy(request.pc))).status:
			return rres.errorResultCopy()

		#
		# Handle RCN's
		#

		tpe = resource.tpe
		if request.rcn is None or request.rcn == ResultContentType.attributes:	# rcn is an int
			return res
		elif request.rcn == ResultContentType.modifiedAttributes:
			dictNew = deepcopy(resource.dict)
			requestPC = request.pc[tpe]
			# return only the modified attributes. This does only include those attributes that are updated differently, or are
			# changed by the CSE, then from the original request. Luckily, all key/values that are touched in the update request
			#  are in the resource's __modified__ variable.
			return Result(status = res.status, resource = { tpe : Utils.resourceModifiedAttributes(dictOrg, dictNew, requestPC, modifiers = resource[Resource._modified]) }, rsc = res.rsc)
		elif request.rcn == ResultContentType.nothing:
			return Result(status = res.status, rsc = res.rsc)
		# TODO C.rcnDiscoveryResultReferences 
		else:
			return Result.errorResult(dbg = 'wrong rcn for UPDATE')


	def updateLocalResource(self, resource:Resource, 
								  dct:Optional[JSON] = None, 
								  doUpdateCheck:Optional[bool] = True, 
								  originator:Optional[str] = None) -> Result:
		"""	Update a resource in the CSE. Call update() and updated() callbacks on the resource.
		
			Args:
				resource: Resource to update.
				dct: JSON dictionary with the updated attributes.
				doUpdateCheck: Enable/disable a call to update().
				originator: The request's originator.
			Return:
				Result object.
		"""
		L.isDebug and L.logDebug(f'Updating resource ri: {resource.ri}, type: {resource.ty}')
		if doUpdateCheck:
			if not (res := resource.willBeUpdated(dct, originator)).status:
				return res
			if not (res := resource.update(dct, originator)).status:
				# return res.errorResultCopy()
				return res
		else:
			L.isDebug and L.logDebug('No check, skipping resource update')

		# Signal a successful update so that further actions can be taken
		resource.updated(dct, originator)

		# send a create event
		CSE.event.updateResource(resource)		# type: ignore
		return resource.dbUpdate()


	def updateResourceFromDict(self, dct:JSON, 
									 id:str, 
									 originator:Optional[str] = None, 
									 resource:Optional[Resource] = None) -> Result:
		# TODO doc

		# Update locally
		if (rID := Utils.localResourceID(id)) is not None:
			L.isDebug and L.logDebug(f'Updating local resource with ID: {id} originator: {originator}')

			# Retrieve the resource if not given
			if resource is None:
				if not (res := self.retrieveLocalResource(rID, originator = originator)).status:
					L.isDebug and L.logDebug(f'Cannot retrieve resource: {rID}')
					return res
				resource = res.resource
			
			# Check Permission
			if not CSE.security.hasAccess(originator, resource, Permission.UPDATE):
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = L.logDebug(f'Originator: {originator} has no UPDATE access to: {resource.ri}'))

			# Update it locally
			if not (res := self.updateLocalResource(resource, dct, originator = originator)).status:
				return res

		# Update remotely
		else:
			L.isDebug and L.logDebug(f'Updating remote resource with ID: {id} originator: {originator}')
			if not (res := CSE.request.sendUpdateRequest(id, originator = originator, content = dct)).status:
				return res
		
			# The request might have gone through normally and returned, but might still have failed on the remote CSE.
			# We need to set the status and the dbg attributes and return
			if res.rsc != ResponseStatusCode.updated:
				res.status = False
				res.dbg = res.request.pc.get('dbg')
				return res

		# Return success and updated resource 
		return res


	#########################################################################
	#
	#	Delete resources
	#

	def processDeleteRequest(self, request:CSERequest, 
								   originator:str, 
								   id:Optional[str] = None) -> Result:
		"""	Process a DELETE request. Delete resource(s).

			Args:
				request: The incoming request.
				originator: The requests originator.
				id: Optional ID of the request.
			Return:
				Result object.
		"""
		L.isDebug and L.logDebug(f'Process DELETE request for id: {request.id}|{request.srn}')

		# handle transit requests
		if Utils.localResourceID(request.id) is None and Utils.localResourceID(request.srn) is None:
			return CSE.request.handleTransitDeleteRequest(request)

		fopsrn, id = self._checkHybridID(request, id) # overwrite id if another is given

		# Unknown resource ?
		if not id and not fopsrn:
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = L.logDebug('resource not found'))

		# Handle operation execution time and check request expiration
		self._handleOperationExecutionTime(request)
		if not (res := self._checkRequestExpiration(request)).status:
			return res

		# handle fanout point requests
		if (fanoutPointResource := Utils.fanoutPointResource(fopsrn)) and fanoutPointResource.ty == ResourceTypes.GRP_FOPT:
			L.isDebug and L.logDebug(f'Redirecting request to fanout point: {fanoutPointResource.getSrn()}')
			return fanoutPointResource.handleDeleteRequest(request, fopsrn, request.originator)

		# get resource to be removed and check permissions
		if not (res := self.retrieveResource(id)).resource:
			L.isDebug and L.logDebug(res.dbg)
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = res.dbg)
		resource = cast(Resource, res.resource)

		if CSE.security.hasAccess(originator, resource, Permission.DELETE) == False:
			return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = 'originator has no privileges for DELETE')

		# Check for virtual resource
		if resource.isVirtual():
			return resource.handleDeleteRequest(request, id, originator)	# type: ignore[no-any-return]

		#
		# Handle RCN's first. Afterward the resource & children are no more
		#

		resultContent:Resource|JSON = None
		if request.rcn is None or request.rcn == ResultContentType.nothing:	# rcn is an int
			resultContent = None
		elif request.rcn == ResultContentType.attributes:
			resultContent = resource
		# resource and child resources, full attributes
		elif request.rcn == ResultContentType.attributesAndChildResources:
			children = self.discoverChildren(id, resource, originator, request.fc, Permission.DELETE)
			self._childResourceTree(children, resource)	# the function call add attributes to the result resource. Don't use the return value directly
			resultContent = resource
		# direct child resources, NOT the root resource
		elif request.rcn == ResultContentType.childResources:
			children = self.discoverChildren(id, resource, originator, request.fc, Permission.DELETE)
			childResources:JSON = { resource.tpe : {} }			# Root resource as a dict with no attributes
			self.resourceTreeDict(children, childResources[resource.tpe])
			resultContent = childResources
		elif request.rcn == ResultContentType.attributesAndChildResourceReferences:
			children = self.discoverChildren(id, resource, originator, request.fc, Permission.DELETE)
			self._resourceTreeReferences(children, resource, request.drt, 'ch')	# the function call add attributes to the result resource
			resultContent = resource
		elif request.rcn == ResultContentType.childResourceReferences: # child resource references
			children = self.discoverChildren(id, resource, originator, request.fc, Permission.DELETE)
			childResourcesRef:JSON = { resource.tpe: {} }  # Root resource with no attribute
			self._resourceTreeReferences(children, childResourcesRef[resource.tpe], request.drt, 'm2m:rrl')
			resultContent = childResourcesRef
		# TODO RCN.discoveryResultReferences
		else:
			return Result.errorResult(rsc = ResponseStatusCode.badRequest, dbg = 'wrong rcn for DELETE')

		# remove resource
		res = self.deleteLocalResource(resource, originator, withDeregistration = True)
		return Result(status = res.status, resource = resultContent, rsc = res.rsc, dbg = res.dbg)


	def deleteLocalResource(self, resource:Resource, 
								  originator:Optional[str] = None, 
								  withDeregistration:Optional[bool] = False, 
								  parentResource:Optional[Resource] = None, 
								  doDeleteCheck:Optional[bool] = True) -> Result:
		L.isDebug and L.logDebug(f'Removing resource ri: {resource.ri}, type: {resource.ty}')

		resource.deactivate(originator)	# deactivate it first

		# Check resource deletion
		if withDeregistration:
			if not (res := CSE.registration.checkResourceDeletion(resource)).status:
				return Result.errorResult(dbg = res.dbg)

		# Retrieve the parent resource now, because we need it later
		if not parentResource:
			parentResource = resource.retrieveParentResource()

		# delete the resource from the DB. Save the result to return later
		res = resource.dbDelete()

		# send a delete event
		CSE.event.deleteResource(resource) 	# type: ignore

		# Now notify the parent resource
		if doDeleteCheck and parentResource:
			parentResource.childRemoved(resource, originator)

		return Result(status = res.status, resource = resource, rsc = res.rsc, dbg = res.dbg)



	def deleteResource(self, id:str, 
							 originator:Optional[str] = None) -> Result:
		# TODO doc

		
		# Update locally
		if (rID := Utils.localResourceID(id)) is not None:
			L.isDebug and L.logDebug(f'Deleting local resource with ID: {id} originator: {originator}')

			# Retrieve the resource
			if not (res := self.retrieveLocalResource(rID, originator = originator)).status:
				L.isDebug and L.logDebug(f'Cannot retrieve resource: {rID}')
				return res
			
			# Check Permission
			if not CSE.security.hasAccess(originator, res.resource, Permission.DELETE):
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = L.logDebug(f'Originator: {originator} has no DELETE access to: {res.resource.ri}'))

			# Update it locally
			if not (res := self.deleteLocalResource(res.resource, originator = originator)).status:
				return res

		# Delete remotely
		else:
			L.isDebug and L.logDebug(f'Deleting remote resource with ID: {id} originator: {originator}')
			if not (res := CSE.request.sendDeleteRequest(id, originator = originator)).status:
				return res
		
		# Return success
		return res


	#########################################################################
	#
	#	Notify
	#

	def processNotifyRequest(self, request:CSERequest, 
								   originator:Optional[str], 
								   id:Optional[str] = None) -> Result:
		"""	Process a NOTIFY request. Send notifications to resource(s).

			Args:
				request: The incoming request.
				originator: The requests originator.
				id: Optional ID of the request.
			Return:
				Result object.
		"""
		L.isDebug and L.logDebug(f'Process NOTIFY request for id: {request.id}|{request.srn}')

		# handle transit requests
		if Utils.localResourceID(request.id) is None:
			return CSE.request.handleTransitNotifyRequest(request)

		srn, id = self._checkHybridID(request, id) # overwrite id if another is given

		# Handle operation execution time and check request expiration
		self._handleOperationExecutionTime(request)
		if not (res := self._checkRequestExpiration(request)).status:
			return res

		# get resource to be notified and check permissions
		if not (res := self.retrieveResource(id)).resource:
			L.isDebug and L.logDebug(res.dbg)
			return Result.errorResult(rsc = ResponseStatusCode.notFound, dbg = res.dbg)
		targetResource = res.resource

		# Security checks below

		
		# Check for <pollingChannelURI> resource
		# This is also the only resource type supported that can receive notifications, yet
		if targetResource.ty == ResourceTypes.PCH_PCU :
			if not CSE.security.hasAccessToPollingChannel(originator, targetResource):
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = L.logDebug(f'Originator: {originator} has not access to <pollingChannelURI>: {id}'))
			return targetResource.handleNotifyRequest(request, originator)	# type: ignore[no-any-return]

		if ResourceTypes.isNotificationEntity(targetResource.ty):
			if not CSE.security.hasAccess(originator, targetResource, Permission.NOTIFY):
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = L.logDebug('fOriginator has no NOTIFY privilege for: {id}'))
			#  A Notification to one of these resources will always be a Received Notify Request
			return CSE.request.handleReceivedNotifyRequest(id, request = request, originator = originator)
		
		if targetResource.ty == ResourceTypes.CRS:
			return targetResource.handleNotification(request, originator)

		# error
		return Result.errorResult(dbg = L.logDebug(f'Unsupported resource type: {targetResource.ty} for notifications.'))


	def notifyLocalResource(self, ri:str, 
								  originator:str, 
								  content:JSON) -> Result:
		# TODO doc

		L.isDebug and L.logDebug(f'Sending NOTIFY to local resource: {ri}')
		if not (res := self.retrieveLocalResource(ri, originator =originator)).status:
			return res

		# Check Permission
		if not CSE.security.hasAccess(originator, res.resource, Permission.NOTIFY):
			return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = L.logDebug(f'Originator: {originator} has no NOTIFY access to: {res.resource.ri}'))
		
		# Send notification
		request = CSERequest(id = ri,
							op = Operation.NOTIFY,
							originator = originator,
							ot = DateUtils.getResourceDate(),
							rqi = Utils.uniqueRI(),
							rvi = CSE.releaseVersion,
							pc = content)
		return res.resource.handleNotification(request, originator)
		


	#########################################################################
	#
	#	Public Utility methods
	#

	def directChildResources(self, pi:str, 
								   ty:Optional[ResourceTypes] = None) -> list[Resource]:
		"""	Return all child resources of a resource, optionally filtered by type.
			An empty list is returned if no child resource could be found.
		"""
		return cast(List[Resource], CSE.storage.directChildResources(pi, ty))


	def countDirectChildResources(self, pi:str, ty:ResourceTypes = None) -> int:
		"""	Return the number of all child resources of resource, optionally filtered by type. 
		"""
		return CSE.storage.countDirectChildResources(pi, ty)


	def retrieveLatestOldestInstance(self, pi:str, 
										   ty:ResourceTypes, 
										   oldest:Optional[bool] = False) -> Optional[Resource]:
		"""	Get the latest or oldest x-Instance resource for a parent.

			Args:
				pi: parent resourceIdentifier
				ty: resource type to look for
				oldest: switch between oldest and latest search
			
			Return:
				Resource
		"""
		result = None
		if oldest:
			result = CSE.storage.retrieveOldestResource(ty = int(ty), pi = pi)
		else:
			result = CSE.storage.retrieveLatestResource(ty = int(ty), pi = pi)
			
		if result == None:
			return None

		# Instantiate and return resource
		return Factory.resourceFromDict(result).resource


	def discoverChildren(self, id:str, 
							   resource:Resource, 
							   originator:str, 
							   filterCriteria:FilterCriteria, 
							   permission:Permission) -> Optional[list[Resource]]:
		# TODO documentation
		if not (res := self.discoverResources(id, originator, filterCriteria = filterCriteria, rootResource = resource, permission = permission)).status:
			return None
		# check and filter by ACP
		children = []
		for r in cast(List[Resource], res.data):
			if CSE.security.hasAccess(originator, r, permission):
				children.append(r)
		return children


	def countResources(self, ty:ResourceTypes|Tuple[ResourceTypes, ...]=None) -> int:
		""" Return total number of resources.
			Optional filter by type.
		"""
		# Count all resources
		if ty == None:	# ty is an int
			return CSE.storage.countResources()
		elif isinstance(ty, tuple):
			return CSE.storage.countResources(ty)
		else:
			return CSE.storage.countResources( (ty,) )


	def retrieveResourcesByType(self, ty:ResourceTypes) -> list[Resource]:
		""" Retrieve all resources of a type. 

			Args:
				ty: Resouce type to search for.
			Return:
				A list of retrieved `Resource` objects. This list might be empty.
		"""
		result = []
		rss = CSE.storage.retrieveResourcesByType(ty)
		for rs in (rss or []):
			result.append(Factory.resourceFromDict(rs).resource)
		return result
	

	def deleteChildResources(self, parentResource:Resource, 
								   originator:str, 
								   ty:Optional[ResourceTypes] = None,
								   doDeleteCheck:Optional[bool] = True) -> None:
		"""	Remove all child resources of a parent recursively. 

			If *ty* is set only the resources of this type are removed.
		"""
		# Remove directChildResources
		rs = self.directChildResources(parentResource.ri)
		for r in rs:
			if ty is None or r.ty == ty:	# ty is an int
				#parentResource.childRemoved(r, originator)	# recursion here
				self.deleteLocalResource(r, originator, parentResource = parentResource, doDeleteCheck = doDeleteCheck)

	#########################################################################
	#
	#	Request execution utilities
	#

	def _handleOperationExecutionTime(self, request:CSERequest) -> None:
		"""	Handle operation execution time and request expiration. If the OET is set then
			wait until the provided timestamp is reached.

			Args:
				request: The request to check.
		"""
		if request.oet:
			# Calculate the dealy
			delay = DateUtils.timeUntilAbsRelTimestamp(request.oet)
			L.isDebug and L.logDebug(f'Waiting: {delay:.4f} seconds until delayed execution')
			# Just wait some time
			DateUtils.waitFor(delay)	


	def _checkRequestExpiration(self, request:CSERequest) -> Result:
		"""	Check request expiration timeout if a request timeout is give.

			Args:
				request: The request to check.
			Return:
				 A negative Result status when the timeout timestamp has been reached or passed.
		"""
		if request._rqetUTCts is not None and DateUtils.timeUntilTimestamp(request._rqetUTCts) <= 0.0:
			return Result.errorResult(rsc = ResponseStatusCode.requestTimeout, dbg = L.logDebug('Request timed out'))
		return Result.successResult()



	#########################################################################
	#
	#	Internal methods for collecting resources and child resources into structures
	#

	def _resourcesToURIList(self, resources:list[Resource], drt:int) -> JSON:
		"""	Create a m2m:uril structure from a list of resources.
		"""
		cseid = f'{CSE.cseCsi}/'	# SP relative. csi already starts with a "/"
		lst = []
		for r in resources:
			lst.append(Utils.structuredPath(r) if drt == DesiredIdentifierResultType.structured else cseid + r.ri)
		return { 'm2m:uril' : lst }


	def resourceTreeDict(self, resources:list[Resource], targetResource:Resource|JSON) -> list[Resource]:
		"""	Recursively walk the results and build a sub-resource tree for each resource type.
		"""
		rri = targetResource['ri'] if 'ri' in targetResource else None
		while True:		# go multiple times per level through the resources until the list is empty
			result = []
			handledTy = None
			handledTPE = None
			idx = 0
			while idx < len(resources):
				r = resources[idx]

				if rri and r.pi != rri:	# only direct children
					idx += 1
					continue
				if r.isVirtual():	# Skip latest, oldest etc virtual resources
					idx += 1
					continue
				if handledTy is None:					# ty is an int
					handledTy = r.ty					# this round we check this type
					handledTPE = r.tpe					# ... and this TPE (important to distinguish specializations in mgmtObj and fcnt )
				if r.ty == handledTy and r.tpe == handledTPE:		# handle only resources of the currently handled type and TPE!
					result.append(r)					# append the found resource 
					resources.remove(r)						# remove resource from the original list (greedy), but don't increment the idx
					resources = self.resourceTreeDict(resources, r)	# check recursively whether this resource has children
				else:
					idx += 1							# next resource

			# add all found resources under the same type tag to the rootResource
			if len(result) > 0:
				# sort resources by type and then by lowercase rn
				if self.sortDiscoveryResources:
					# result.sort(key=lambda x:(x.ty, x.rn.lower()))
					result.sort(key = lambda x: (x.ty, x.ct) if ResourceTypes.isInstanceResource(x.ty) else (x.ty, x.rn.lower()))
				targetResource[result[0].tpe] = [r.asDict(embedded = False) for r in result]
				# TODO not all child resources are lists [...] Handle just to-1 relations
			else:
				break # end of list, leave while loop
		return resources # Return the remaining list


	def _resourceTreeReferences(self, resources:list[Resource], 
									  targetResource:Resource|JSON, 
									  drt:Optional[DesiredIdentifierResultType] = DesiredIdentifierResultType.structured,
									  tp:Optional[str] = 'm2m:rrl') -> Resource|JSON:
		""" Retrieve child resource references of a resource and add them to
			a new target resource as "children" """
		if not targetResource:
			targetResource = { }

		t = []

		# sort resources by type and then by lowercase rn
		if self.sortDiscoveryResources:
			resources.sort(key = lambda x:(x.ty, x.rn.lower()))
		
		for r in resources:
			if ResourceTypes.isVirtualResource(r.ty):	# Skip virtual resources
				continue
			ref = { 'nm' : r['rn'], 
					'typ' : r['ty'], 
					'val' : Utils.toSPRelative(Utils.structuredPath(r) if drt == DesiredIdentifierResultType.structured else r.ri)
			}
			if r.ty == ResourceTypes.FCNT:
				ref['spty'] = r.cnd		# TODO Is this correct? Actually specializationID in TS-0004 6.3.5.29, but this seems to be wrong
			t.append(ref)

		# The following reflects a current inconsistency in the standard.
		# If this list of childResourceReferences is for rcn=5 (attributesAndChildResourceReferences), then the structure
		# is -> 'ch' : [ <listOfChildResourceRef> ]
		# If this list of childResourceReferences is for rcn=6 (childResourceReferences), then the structure 
		# is -> '{ 'rrl' : { 'rrf' : [ <listOfChildResourceRef> ]}}  ( an extra rrf struture )
		targetResource[tp] = { "rrf" : t } if tp == 'm2m:rrl' else t
		return targetResource


	# Retrieve full child resources of a resource and add them to a new target resource
	def _childResourceTree(self, resources:list[Resource], targetResource:Resource|JSON) -> None:
		if len(resources) == 0:
			return
		result:JSON = {}
		self.resourceTreeDict(resources, result)	# rootResource is filled with the result
		for k,v in result.items():			# copy child resources to result resource
			targetResource[k] = v


	#########################################################################
	#
	#	Internal methods for ID handling
	#

	def _checkHybridID(self, request:CSERequest, id:str) -> Tuple[str, str]:
		"""	Return a corrected *id* and *srn* in case this is a hybrid ID.

			Args:
				request: A request object that provides *id* and *srn*. *srn* might be None.
				id: An ID which might be None. If it is not None, then it will be taken to generate the *srn*.
			Return:
				Tuple of *srn* and *id*
		"""
		if id:
			srn = id if Utils.isStructured(id) else None # Overwrite srn if id is strcutured. This is a bit mixed up sometimes
			return Utils.srnFromHybrid(srn, id) # Hybrid
			# return Utils.srnFromHybrid(None, id) # Hybrid
		return Utils.srnFromHybrid(request.srn, request.id) # Hybrid

