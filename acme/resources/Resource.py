#
#	Resource.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#

""" Base class for all oneM2M resource types.
"""

# The following import allows to use "Resource" inside a method typing definition
from __future__ import annotations
from typing import Any, Tuple, cast, Optional

import json

from copy import deepcopy

from ..etc.Types import ResourceTypes, Result, NotificationEventType, ResponseStatusCode, CSERequest, JSON
from ..etc import Utils
from ..etc import DateUtils
from ..services.Logging import Logging as L
from ..services import CSE

# Future TODO: Check RO/WO etc for attributes (list of attributes per resource?)
# TODO cleanup optimizations
# TODO _remodeID - is anybody using that one??




class Resource(object):
	""" Base class for all oneM2M resource types,
	
		Attributes:

	"""

	# Contstants for internal attributes
	_rtype 				= '__rtype__'
	"""	Constant: Name of the internal *__rtype__* attribute. This attribute holds the resource type name, e.g. "m2m:cnt". """

	_srn				= '__srn__'
	"""	Constant: Name of the internal *__srn__* attribute. This attribute holds the resource's structured resource name. """

	_node				= '__node__'
	"""	Constant: Name of the internal __node__ attribute. This attribute is used in some resource types to hold a reference to the hosting <node> resource. """

	_createdInternally	= '__createdinternally__'	# TODO better name. This is actually an RI
	""" Constant: Name of the *__createdInternally__* attribute. This attribute indicates whether a resource was created internally or by an external request. """

	_imported			= '__imported__'
	""" Constant: Name of the *__imported__* attribute. This attribute indicates whether a resource was imported or created by a script, of created by a request. """

	_announcedTo 		= '__announcedto__'			# List
	""" Constant: Name of the *__announcedTo__* attribute. This attribute holds internal announcement information. """

	_isInstantiated		= '__isinstantiated__'
	""" Constant: Name of the *__isInstantiated__* attribute. This attribute indicates whether a resource is instantiated. """

	_originator			= '__originator__'			# Or creator
	""" Constant: Name of the *__originator__* attribute. This attribute holds the original creator of a resource."""

	_modified			= '__modified__'
	""" Constant: Name of the *__modified__* attribute. This attribute holds the resource's precise modification timestamp. """

	_remoteID			= '__remoteid__'			# When this is a resource from another CSE
	""" Constant: Name of the *__remoteid__* attribute. This attribute holds a list of the resource's announced variants. """

	_rvi				= '__rvi__'					# Request version indicator when created
	""" Constant: Name of the *__rvi__* attribute. This attribute holds the Release Version Indicator for which the resource was created. """
 
	_isVirtual			= '__isvirtual__'
	""" COnstant: Name of the *__isvirtual__* attribute. This attribute holds boolean value indicating resource either virtual resource or not"""

	_index				= "index"
	# Postgres id increment in resources table
 
	_resource_index		= "resource_index"
	# Postgres id increment in resource type specific table
 
	_excludeFromUpdate = [ 'ri', 'ty', 'pi', 'ct', 'lt', 'st', 'rn', 'mgd', _index, _resource_index, _isVirtual ]
	"""	Resource attributes that are excluded when updating the resource from request update call"""
 
	_excludeFromDBUpdate = [ 'ri', 'ty', 'pi', 'ct', 'rn', 'mgd', _index, _resource_index, _isVirtual]
	""" Resource attributes that are excluded when execute DB update"""

	# ATTN: There is a similar definition in FCNT, TSB, and others! Don't Forget to add attributes there as well
	internalAttributes	= [ _rtype, _srn, _node, _createdInternally, _imported, _resource_index, _index,
							_isInstantiated, _originator, _announcedTo, _modified, _remoteID, _rvi, _isVirtual ]
	"""	List of internal attributes and which do not belong to the oneM2M resource attributes """

	universalCommonAttributes = [ "ty", "ri", "rn", "pi", "ct", "lt", "acpi", "et", "st", "at", "aa", "lbl",
                              	 "esi", "daci", "cr", "cstn" ]
	""" List of universal and common attributes of resources in shortname"""

	def __init__(self, 
				 ty:ResourceTypes, 
				 dct:JSON, 
				 pi:Optional[str] = None, 
				 tpe:Optional[str] = None,
				 create:Optional[bool] = False,
				 inheritACP:Optional[bool] = False, 
				 readOnly:Optional[bool] = False, 
				 rn:Optional[str] = None) -> None:
		"""	Initialization of a Resource instance.
		
			Args:
				ty: Mandatory resource type.
				dct: Mandatory resource attributes.
				pi: Optional parent resource identifier.
				tpe: Optional domain and resource name.
				create: Optional indicator whether this resource is just created or an instance of an existing resource.
				inheritACP: Optional indicator whether this resource inherits *acpi* attribute from its parent (if any).
				readOnly: Optional indicator whether this resource is read-only.
				rn: Optional resource name. If none is given and the resource is created, then a random name is assigned to the resource.
		"""

		self.tpe = tpe
		"""	The resource's domain and type name. """
		self.readOnly	= readOnly
		"""	Flag set during creation of a resource instance whether a resource type allows only read-only access to a resource. """
		self.inheritACP	= inheritACP
		"""	Flag set during creation of a resource instance whether a resource type inherits the `resources.ACP.ACP` from its parent resource. """
		self.dict 		= {}
		"""	Dictionary for public and internal resource attributes. """
		self.isImported	= False
		"""	Flag set during creation of a resource instance whether a resource is imported, which disables some validation checks. """
		self._originalDict = {}
		"""	When retrieved from the database: Holds a temporary version of the resource attributes as they were read from the database. """

		# For some types the tpe/root is empty and will be set later in this method
		if ty not in [ ResourceTypes.FCNT, ResourceTypes.FCI ]: 	
			self.tpe = ty.tpe() if not tpe else tpe

		if dct is not None: 
			self.isImported = dct.get(self._imported)	# might be None, or boolean
			self.dict = deepcopy(dct.get(self.tpe))
			if not self.dict:
				self.dict = deepcopy(dct)
			self._originalDict = deepcopy(dct)	# keep for validation in activate() later
		else:
			# no Dict, so the resource is instantiated programmatically
			self.setAttribute(self._isInstantiated, True)

		# if self.dict is not None:
		if not self.tpe: 
			self.tpe = self.__rtype__
		if not self.hasAttribute('ri'):
			self.setAttribute('ri', Utils.uniqueRI(self.tpe), overwrite = False)
		if pi is not None: # test for None bc pi might be '' (for cse). pi is used subsequently here
			self.setAttribute('pi', pi)

		# override rn if given
		if rn:
			self.setResourceName(rn)

		# Create an RN if there is none (not given, none in the resource)
		if not self.hasAttribute('rn'):	# a bit of optimization bc the function call might cost some time
			self.setResourceName(Utils.uniqueRN(self.tpe))

		# Check uniqueness of ri. otherwise generate a new one. Only when creating
		if create:
			while not Utils.isUniqueRI(ri := self.ri):
				L.isWarn and L.logWarn(f'RI: {ri} is already assigned. Generating new RI.')
				self['ri'] = Utils.uniqueRI(self.tpe)

		# Set some more attributes
		if not (self.hasAttribute('ct') and self.hasAttribute('lt')):
			ts = DateUtils.getResourceDate()
			self.setAttribute('ct', ts, overwrite = False)
			self.setAttribute('lt', ts, overwrite = False)

		# Handle resource type
		if ty not in [ ResourceTypes.CSEBase ] and not self.hasAttribute('et'):
			self.setAttribute('et', DateUtils.getResourceDate(CSE.request.maxExpirationDelta), overwrite = False) 
		if ty is not None:
			self.setAttribute('ty', int(ty))

		#
		## Note: ACPI is handled in activate() and update()
		#

		# Remove empty / null attributes from dict
		# But see also the comment in update() !!!
		self.dict = Utils.removeNoneValuesFromDict(self.dict, ['cr'])	# allow the cr attribute to stay in the dictionary. It will be handled with in the RegistrationManager

		self[self._rtype] = self.tpe
		self.setAttribute(self._announcedTo, [], overwrite = False)
		self.setAttribute(self._isVirtual, self.isVirtual())


	# Default encoding implementation. Overwrite in subclasses
	def asDict(self, embedded:Optional[bool] = True, 
					 update:Optional[bool] = False, 
					 noACP:Optional[bool] = False) -> JSON:
		"""	Get the JSON resource representation.
		
			Args:
				embedded: Optional indicator whether the resource should be embedded in another resource structure. In this case it is *not* embedded in its own "domain:name" structure.
				update: Optional indicator whether only the updated attributes shall be included in the result.
				noACP: Optional indicator whether the *acpi* attribute shall be included in the result.
			
			Return:
				A `JSON` object with the resource representation.
		"""
		# remove (from a copy) all internal attributes before printing
		dct = { k:deepcopy(v) for k,v in self.dict.items() 				# Copy k:v to the new dictionary, ...
					if k not in self.internalAttributes 				# if k is not in internal attributes (starting with __), AND
					and not (noACP and k == 'acpi')						# if not noACP is True and k is 'acpi', AND
					and not (update and k in self._excludeFromUpdate) 	# if not update is True and k is in _excludeFromUpdate)
				}

		return { self.tpe : dct } if embedded else dct


	def activate(self, parentResource:Resource, originator:str) -> Result:
		"""	This method is called to activate a resource, usually in a CREATE request.

			This is not always the case, e.g. when a resource object is just used temporarly.
			**NO** notification on activation/creation happens in this method!

			This method is implemented in sub-classes as well.
			
			Args:
				parentResource: The resource's parent resource.
				originator: The request's originator.
			Return:
				Result object indicating success or failure.
		"""
		# TODO check whether 				CR is set in RegistrationManager
		L.isDebug and L.logDebug(f'Activating resource: {self.ri}')

		# validate the attributes but only when the resource is not instantiated.
		# We assume that an instantiated resource is always correct
		# Also don't validate virtual resources
		if not self[self._isInstantiated] and not self.isVirtual() :
			if not (res := CSE.validator.validateAttributes(self._originalDict, self.tpe, self.ty, self._attributes, isImported = self.isImported, createdInternally = self.isCreatedInternally(), isAnnounced = self.isAnnounced())).status:
				return res

		# validate the resource logic
		if not (res := self.validate(originator, create = True, parentResource = parentResource)).status:
			return res
		self.dbUpdate() # TODO: Why need to call update here?
		
		# Various ACPI handling
		# ACPI: Check <ACP> existence and convert <ACP> references to CSE relative unstructured
		if self.acpi is not None and not self.isAnnounced():
			# Test wether an empty array is provided				
			if len(self.acpi) == 0:
				return Result.errorResult(dbg = 'acpi must not be an empty list')
			if not (res := self._checkAndFixACPIreferences(self.acpi)).status:
				return res
			self.setAttribute('acpi', res.data)

		self.setAttribute(self._originator, originator, overwrite = False)
		self.setAttribute(self._rtype, self.tpe, overwrite = False) 

		# return Result(status = True, rsc = RC.OK)
		return Result.successResult()


	def deactivate(self, originator:str) -> None:
		"""	Deactivate an active resource.

			This usually happens when creating the resource via a request.
			A subscription check for deletion is performed.

			This method is implemented in sub-classes as well.

			Args:
				originator: The requests originator that let to the deletion of the resource.
		"""
		L.isDebug and L.logDebug(f'Deactivating and removing sub-resources for: {self.ri}')
		# First check notification because the subscription will be removed
		# when the subresources are removed
		CSE.notification.checkSubscriptions(self, NotificationEventType.resourceDelete)
		
		# Remove directChildResources. Don't do checks (e.g. subscriptions) for the sub-resources
		CSE.dispatcher.deleteChildResources(self, originator, doDeleteCheck = False)
		
		# Removal of a deleted resource from group(s) is done 
		# asynchronously in GroupManager, triggered by an event.


	def update(self, dct:Optional[JSON] = None, 
					 originator:Optional[str] = None, 
					 doValidateAttributes:Optional[bool] = True) -> Result:
		"""	Update, add or remove resource attributes.

			A subscription check for update is performed.

			This method is implemented in sub-classes as well.

			Args:
				dct: An optional JSON dictionary with the attributes to be updated.
				originator: The optional requests originator that let to the update of the resource.
				doValidateAttributes: If *True* optionally call the resource's `validate()` method.

			Return:
				Result object indicating success or failure.
		"""
		dictOrg = deepcopy(self.dict)	# Save for later for notification

		updatedAttributes = None
		if dct:
			if self.tpe not in dct and self.ty not in [ResourceTypes.FCNTAnnc]:	# Don't check announced versions of announced FCNT
				L.isWarn and L.logWarn("Update type doesn't match target")
				return Result.errorResult(rsc = ResponseStatusCode.contentsUnacceptable, dbg = 'resource types mismatch')

			# validate the attributes
			if doValidateAttributes and not (res := CSE.validator.validateAttributes(dct, self.tpe, self.ty, self._attributes, create = False, createdInternally = self.isCreatedInternally(), isAnnounced = self.isAnnounced())).status:
				return res

			if self.ty not in [ResourceTypes.FCNTAnnc]:
				updatedAttributes = dct[self.tpe] # get structure under the resource type specifier
			else:
				updatedAttributes = Utils.findXPath(dct, '{*}')

			# Check that acpi, if present, is the only attribute
			if 'acpi' in updatedAttributes and updatedAttributes['acpi'] is not None:	# No further checks for access here. This has been done before in the Dispatcher.processUpdateRequest()	
																						# Removing acpi by setting it to None is handled in the else:
																						# acpi can be None! Therefore the complicated test
				# Test wether an empty array is provided				
				if len(ua := updatedAttributes['acpi']) == 0:
					return Result.errorResult(dbg = 'acpi must not be an empty list')
				# Check whether referenced <ACP> exists. If yes, change ID also to CSE relative unstructured
				if not (res := self._checkAndFixACPIreferences(ua)).status:
					return res
				
				self.setAttribute('acpi', res.data, overwrite = True) # copy new value or add new attributes

			else:

				# Update other  attributes
				for key in updatedAttributes:
					# Leave out some attributes
					if key in ['ct', 'lt', 'pi', 'ri', 'rn', 'st', 'ty']:
						continue
					value = updatedAttributes[key]

					# Special handling for et when deleted/set to Null: set a new et
					if key == 'et' and not value:
						self['et'] = DateUtils.getResourceDate(CSE.request.maxExpirationDelta)
						continue
					self.setAttribute(key, value, overwrite = True) # copy new value or add new attributes
			

		# Update lt for those resources that have these attributes
		if 'lt' in self.dict:	# Update the lastModifiedTime
			self['lt'] = DateUtils.getResourceDate()

		# Remove empty / null attributes from dict
		# 2020-08-10 : 	TinyDB doesn't overwrite the whole document but makes an attribute-by-attribute 
		#				update. That means that removed attributes are NOT removed. There is now a 
		#				procedure in the Storage component that removes nulled attributes as well.
		#self.dict = {k: v for (k, v) in self.dict.items() if v is not None }

		# Do some extra validations, if necessary
		if not (res := self.validate(originator, dct = dct)).status:
			return res

		# store last modified attributes
		self[self._modified] = Utils.resourceDiff(dictOrg, self.dict, updatedAttributes)

		# Check subscriptions
		CSE.notification.checkSubscriptions(self, NotificationEventType.resourceUpdate, modifiedAttributes = self[self._modified])
		self.dbUpdate()

		# Check Attribute Trigger
		# TODO CSE.action.checkTrigger, self, modifiedAttributes=self[self._modified])

		# Notify parent that a child has been updated
		if not (parent := cast(Resource, self.retrieveParentResource())):
			return Result.errorResult(rsc = ResponseStatusCode.internalServerError, dbg = L.logErr(f'cannot retrieve parent resource'))
		parent.childUpdated(self, updatedAttributes, originator)

		return Result.successResult()

	def willBeUpdated(self, dct:Optional[JSON] = None, 
							originator:Optional[str] = None, 
							subCheck:Optional[bool] = True) -> Result:
		""" This method is called before a resource will be updated and before calling the `update()` method.
			
			This method is implemented in some sub-classes.

			Args:
				dct: `JSON` dictionary with the attributes that will be updated.
				originator: The request originator.
				subCheck: Optional indicator that a blocking Update shall be performed, if configured.

			Return:
				Result object indicating success or failure.
		"""
		# Perform BlockingUpdate check, and reload resource if necessary
		if not (res := CSE.notification.checkPerformBlockingUpdate(self, originator, dct, finished = lambda: self.dbReloadDict())).status:
			return res
		return Result.successResult()


	def updated(self, dct:Optional[JSON] = None, 
					  originator:Optional[str] = None) -> None:
		"""	Signal to a resource that is was successfully updated. 
		
			This handler can be used to perform	additional actions after the resource was updated, stored etc.
			
			This method is implemented in some sub-classes.

			Args:
				dct: Optional JSON dictionary with the updated attributes.
				originator: The optional request originator.
		"""
		pass


	def willBeRetrieved(self, originator:str, 
							  request:Optional[CSERequest] = None, 
							  subCheck:Optional[bool] = True) -> Result:
		""" This method is called before a resource will be send back in a RETRIEVE response.
			
			This method is implemented in some sub-classes.

			Args:
				originator: The request originator.
				request: The RETRIEVE request.
				subCheck: Optional indicator that a blocking Retrieve shall be performed, if configured.
			Return:
				Result object indicating success or failure.
		"""
		# Check for blockingRetrieve or blockingRetrieveDirectChild
		if subCheck and request:
			if not (res := CSE.notification.checkPerformBlockingRetrieve(self, request, finished = lambda: self.dbReloadDict())).status:
				return res
		return Result.successResult()


	def childWillBeAdded(self, childResource:Resource, originator:str) -> Result:
		""" Called before a child will be added to a resource.
			
			This method is implemented in some sub-classes.

			Args:
				childResource: Resource that will be added as a child to the resource.
				originator: The request originator.
			Return:
				A Result object with status True, or False (in which case the adding will be rejected), and an error code.
		"""
		return Result.successResult()


	def childAdded(self, childResource:Resource, originator:str) -> None:
		""" Called after a child resource was added to the resource.

			This method is implemented in some sub-classes.

			Args:
				childResource: The child resource that was be added as a child to the resource.
				originator: The request originator.
 		"""
		# Check Subscriptions
		CSE.notification.checkSubscriptions(self, NotificationEventType.createDirectChild, childResource)


	def childUpdated(self, childResource:Resource, updatedAttributes:JSON, originator:str) -> None:
		"""	Called when a child resource was updated.
					
			This method is implemented in some sub-classes.
		
			Args:
				childResource: The child resource that was be updates.
				updatedAttributes: JSON dictionary with the updated attributes.
				originator: The request originator.
		"""
		pass


	def childRemoved(self, childResource:Resource, originator:str) -> None:
		""" Called when a child resource of the resource was removed.

			This method is implemented in some sub-classes.

		Args:
			childResource: The removed child resource.
			originator: The request originator.
		"""
		CSE.notification.checkSubscriptions(self, NotificationEventType.deleteDirectChild, childResource)


	def canHaveChild(self, resource:Resource) -> bool:
		""" Check whether *resource* is a valild child resource for this resource. 

		Args:
			resource: The resource to test.
		Return:
			Boolean indicating whether *resource* is a an allowed resorce for this resource.
		"""
		from .Unknown import Unknown # Unknown imports this class, therefore import only here
		return resource.ty in self._allowedChildResourceTypes or isinstance(resource, Unknown)


	def validate(self, originator:Optional[str] = None, 
					   create:Optional[bool] = False, 
					   dct:Optional[JSON] = None, 
					   parentResource:Optional[Resource] = None) -> Result:
		""" Validate a resource. 
		
			Usually called within `activate()` or `update()` methods.

			This method is implemented in some sub-classes.

			Args:
				originator: Optional request originator
				create: Optional indicator whether this is CREATE request
				dct: Updated attributes to validate
				parentResource: The parent resource
			Return:
				A Result object with status True, or False (in which case the request will be rejected), and an error code.
		"""
		L.isDebug and L.logDebug(f'Validating resource: {self.ri}')
		if not ( Utils.isValidID(self.ri) and
				 Utils.isValidID(self.pi, allowEmpty = self.ty == ResourceTypes.CSEBase) and # pi is empty for CSEBase
				 Utils.isValidID(self.rn)):
			return Result.errorResult(rsc = ResponseStatusCode.badRequest, dbg = L.logDebug(f'Invalid ID: ri: {self.ri}, pi: {self.pi}, or rn: {self.rn})'))

		# expirationTime handling
		if et := self.et:
			if self.ty == ResourceTypes.CSEBase:
				return Result.errorResult(dbg = L.logWarn('expirationTime is not allowed in CSEBase'))
			if len(et) > 0 and et < (etNow := DateUtils.getResourceDate()):
				return Result.errorResult(dbg = L.logWarn(f'expirationTime is in the past: {et} < {etNow}'))
			if et > (etMax := DateUtils.getResourceDate(CSE.request.maxExpirationDelta)):
				L.isDebug and L.logDebug(f'Correcting expirationDate to maxExpiration: {et} -> {etMax}')
				self['et'] = etMax
		return Result.successResult()


	#########################################################################

	def createdInternally(self) -> str:
		""" Return the resource.ri for which a resource was created.

			This is done in case a resource must be created as a side-effect when another resource
			is, for example, created.
		
			Return:
				Resource ID of the resource for which this resource has been created, or None.
		"""
		return str(self[self._createdInternally])


	def isCreatedInternally(self) -> bool:
		""" Test whether a resource has been created for another resource.

			Return:
				True if this resource has been created for another resource.
		"""
		return self[self._createdInternally] is not None


	def setCreatedInternally(self, ri:str) -> None:
		"""	Save the resource ID for which this resource was created for.
		
			This has some impacts on internal handling and checks.

			Args:
				ri: Resource ID of the resource for which this resource has been created for.

		"""
		self[self._createdInternally] = ri


	def isAnnounced(self) -> bool:
		""" Test whether a the resource's type is an announced type. 
		
			Returns:
				True if the resource is an announced resource type.
		"""
		return ResourceTypes(self.ty).isAnnounced()

	
	def isVirtual(self) -> bool:
		"""	Test whether the resource is a virtual resource. 

			Return:
				True when the resource is a virtual resource.
		"""
		return ResourceTypes(self.ty).isVirtual()


	#########################################################################
	#
	#	request handler stubs for virtual resources
	#

	def handleRetrieveRequest(self, request:Optional[CSERequest] = None,
									id:Optional[str] = None,
									originator:Optional[str] = None) -> Result:
		"""	Process a RETRIEVE request that is directed to a virtual resource.

			This method **must** be implemented by virtual resource class.
			
			Args:
				request: The request to process.
				id: The structured or unstructured resource ID of the target resource.
				originator: The request's originator.

			Return:
				Result object indicating success or failure.
			"""
		raise NotImplementedError('handleRetrieveRequest()')

	
	def handleCreateRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		"""	Process a CREATE request that is directed to a virtual resource.

			This method **must** be implemented by virtual resource class.
			
			Args:
				request: The request to process.
				id: The structured or unstructured resource ID of the target resource.
				originator: The request's originator.
			Return:
				Result object indicating success or failure.
			"""		
		raise NotImplementedError('handleCreateRequest()')


	def handleUpdateRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		"""	Process a UPDATE request that is directed to a virtual resource.

			This method **must** be implemented by virtual resource class.
			
			Args:
				request: The request to process.
				id: The structured or unstructured resource ID of the target resource.
				originator: The request's originator.
			Return:
				Result object indicating success or failure.
			"""	
		raise NotImplementedError('handleUpdateRequest()')


	def handleDeleteRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		"""	Process a DELETE request that is directed to a virtual resource.

			This method **must** be implemented by virtual resource class.
			
			Args:
				request: The request to process.
				id: The structured or unstructured resource ID of the target resource.
				originator: The request's originator.
			Return:
				Result object indicating success or failure.
			"""
		raise NotImplementedError('handleDeleteRequest()')


	#########################################################################
	#
	#	Attribute handling
	#


	def setAttribute(self, key:str, 
						   value:Any, 
						   overwrite:Optional[bool] = True) -> None:
		"""	Assign a value to a resource attribute.

			If the attribute doesn't exist then it is created.
		
			Args:
				key: The resource attribute's name. This can be a path (see `etc.Utils.setXPath`).
				value: Value to assign to the attribute.
				overwrite: Overwrite the value if already set.
		"""
		Utils.setXPath(self.dict, key, value, overwrite)


	def attribute(self, key:str, 
						default:Optional[Any] = None) -> Any:
		"""	Return the value of an attribute.
		
			Args:
				key: Resource attribute name to look for. This can be a path (see `etc.Utils.findXPath`).
				default: A default value to return if the attribute is not set.
			Return:
				The attribute's value, the *default* value, or None
		"""
		return Utils.findXPath(self.dict, key, default)


	def hasAttribute(self, key:str) -> bool:
		"""	Check whether an attribute exists.

			Args:
				key: Resource attribute name to look for.
			Return:
				Boolean, indicating the existens of an attribute
		"""
		# TODO check sub-elements as well via findXPath
		return key in self.dict


	def delAttribute(self, key:str, 
						   setNone:Optional[bool] = True) -> None:
		""" Delete the attribute 'key' from the resource. 
		
			Args:
				key: Name of the resource attribute name to delete.
				setNone:  By default (*True*) the attribute is not deleted but set to *None* and later removed 
						  when storing the resource in the DB. If *setNone' is *False*, then the attribute is immediately
						  deleted from the resource instance's internal dictionary.
		"""
		if self.hasAttribute(key):
			if setNone:
				self.dict[key] = None
			else:
				del self.dict[key]


	def __setitem__(self, key:str, value:Any) -> None:
		""" Implementation of the *self[key]* operation for assigning to attributes.
		
			It maps to the `setAttribute()` method, and always overwrites existing values.

			Args:
				key: The resource attribute's name. This can be a path (see `etc.Utils.setXPath`).
				value: Value to assign to the attribute.
		"""
		self.setAttribute(key, value)


	def __getitem__(self, key:str) -> Any:
		"""	Implementation of the *self[key|* operation for retrieving attributes.

			It maps to the `attribute()` method, but there is no default value.

			Args:
				key: Resource attribute name to look for. This can be a path (see `etc.Utils.findXPath`).
			Return:
				The attribute's value, or None
		"""
		return self.attribute(key)


	def __delitem__(self, key:str) -> None:
		"""	Implementation of the *self[key|* operation for deleting attributes.

			It maps to the `delAttribute()` method, with *setNone* implicitly set to the default.

			Args:
				key: Resource attribute name to delete. This can be a path (see `etc.Utils.findXPath`).
		"""
		self.delAttribute(key)


	def __contains__(self, key: str) -> bool:
		""" Implementation of the membership test operator.

			It maps to the `hasAttribute()` method.

			Args:
				key: Resource attribute name to test for.
			Return:
				Boolean, indicating the existens of an attribute
		"""
		return self.hasAttribute(key)


	def __getattr__(self, key: str) -> Any:
		""" Map the normal object attribute access to the internal resource attribute dictionary.

			It maps to the `attribute()` method, but there is no default value.

			Args:
				key: Resource attribute name to get.
			Return:
				The attribute's value, or None
		"""
		return self.attribute(key)


	#########################################################################
	#
	#	Attribute specific helpers
	#

	def _normalizeURIAttribute(self, attributeName:str) -> None:
		""" Normalize the URLs in the given attribute.
		
			Various changes are made to the URI in case they are not fully compliant.
			This could be, for example, *poa*, *nu* and other attributes that usually hold a URI.

			If the target attribute is a list of URI then all the URIs in the list are normalized.
			
			Args:
				attributeName: Name of the attribute to normalize.
		"""
		if uris := self[attributeName]:
			if isinstance(uris, list):	# list of uris
				self[attributeName] = [ Utils.normalizeURL(uri) for uri in uris ] 
			else: 							# single uri
				self[attributeName] = Utils.normalizeURL(uris)


	def _checkAndFixACPIreferences(self, acpi:list[str]) -> Result:
		""" Check whether a referenced `ACP` resoure exists, and if yes, change the ID in the list to CSE relative unstructured format.

			Args:
				acpi: List if resource IDs to `ACP` resources.
			Return:
				Result instance. If fully successful (ie. all `ACP` resources exist), then a new list with all IDs converted is returned in *Result.data*.
		"""
		newACPIList =[]
		for ri in acpi:
			if not CSE.importer.isImporting:

				if not (acp := CSE.dispatcher.retrieveResource(ri).resource):
					L.logDebug(dbg := f'Referenced <ACP> resource not found: {ri}')
					return Result.errorResult(dbg = dbg)

					# TODO CHECK TYPE + TEST

				newACPIList.append(acp.ri)
			else:
				newACPIList.append(ri)
		return Result(status = True, data = newACPIList)
	

	def _addToInternalAttributes(self, name:str) -> None:
		"""	Add a *name* to the names of internal attributes. 
		
			*name* is only added if	it is not already present.

			Args:
				name: Attribute name to add.
		"""
		if name not in self.internalAttributes:
			self.internalAttributes.append(name)


	def hasAttributeDefined(self, name:str) -> bool:
		"""	Test wether a resource supports the specified attribute.
		
			Args:
				name: Attribute to test.
			Return:
				Boolean with the result of the test.
		"""
		return self._attributes.get(name) is not None


	#########################################################################
	#
	#	Database functions
	#

	def dbDelete(self) -> Result:
		""" Delete the resource from the database.
		
			Return:
				Result object indicating success or failure.
		 """
		return CSE.storage.deleteResource(self)


	def dbUpdate(self) -> Result:
		""" Update the resource in the database. 

			Return:
				Result object indicating success or failure.
		"""
		return CSE.storage.updateResource(self)


	def dbCreate(self, overwrite:Optional[bool] = False) -> Result:
		"""	Add the resource to the database.
		
			Args:
				overwrite: If true an already existing resource with the same resource ID is overwritten.
			Return:
				Result object indicating success or failure.
		"""
		return CSE.storage.createResource(self, overwrite)


	def dbReload(self) -> Result:
		""" Load a new copy of the same resource from the database. 
			
			The current resource is NOT changed. 
			
			Note:
				The version of the resource in the database might be different, e.g. when the resource instance has been modified but not updated in the database.
			Return:
				Result object indicating success or failure. The resource is returned in the *Result.resource* attribute.		
			"""
		return CSE.storage.retrieveResource(ri = self.ri)


	def dbReloadDict(self) -> Result:
		"""	Reload the resource instance from the database.
		
			The current resource's internal attributes are updated with the versions from the database.

			Return:
				Result object indicating success or failure. The resource is returned as well in the *Result.resource* attribute.		
		 """
		if (res := CSE.storage.retrieveResource(ri = self.ri)).status:
			self.dict = res.resource.dict
		return res


	def validateAttributeValue(self, attributeValue: Any) -> Any:
		return Utils.validateAttributeValue(attributeValue)

	
	def _getInsertGeneralQuery(self) -> str:
		""" Get SQL query of resource universal and common attributes

			It is possible because all universal and common attributes for every resource in 1 database table

		Returns:
			str: Resources table insert query
		"""
		baseQuery = "WITH resource_table AS ({} RETURNING index)"
		resourceQuery = """
					INSERT INTO public.resources(ty, ri, rn, pi, ct, lt, acpi, et, st, at, aa, lbl, esi, daci, cr, cstn, 
						__rtype__, __originator__, __srn__, __announcedto__, __rvi__, __node__, __imported__, __isinstantiated__, __remoteid__, __modified__, __createdinternally__, __isvirtual__)
						VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
			   """
		query = resourceQuery.format(
				self.validateAttributeValue(self.attribute("ty")),
				self.validateAttributeValue(self.attribute("ri")),
				self.validateAttributeValue(self.attribute("rn")),
				self.validateAttributeValue(self.attribute("pi")),
				self.validateAttributeValue(self.attribute("ct")),
				self.validateAttributeValue(self.attribute("lt")),
				self.validateAttributeValue(self.attribute("acpi")),
				self.validateAttributeValue(self.attribute("et")),
				self.validateAttributeValue(self.attribute("st")),
				self.validateAttributeValue(self.attribute("at")),
				self.validateAttributeValue(self.attribute("aa")),
				self.validateAttributeValue(self.attribute("lbl")),
				self.validateAttributeValue(self.attribute("esi")),
				self.validateAttributeValue(self.attribute("daci")),
				self.validateAttributeValue(self.attribute("cr")),
				self.validateAttributeValue(self.attribute("cstn")),
				self.validateAttributeValue(self[self._rtype]),
				self.validateAttributeValue(self[self._originator]),
				self.validateAttributeValue(self[self._srn]),
				self.validateAttributeValue(self[self._announcedTo]),
				self.validateAttributeValue(self[self._rvi]),
				self.validateAttributeValue(self[self._node]),
				self.validateAttributeValue(self[self._imported]),
				self.validateAttributeValue(self[self._isInstantiated]),
				self.validateAttributeValue(self[self._remoteID]),
				self.validateAttributeValue(self[self._modified]),
				self.validateAttributeValue(self[self._createdInternally]),
				self.validateAttributeValue(self[self._isVirtual])
			)
  
		# if resource is not virtual resource, add WITH clause to insert query. Because resource have to insert to another table. See getInsertQuery()
		if not self.isVirtual():
			query = baseQuery.format(query)
  
  
		return query


	def getInsertQuery(self) -> Optional[str]:
		"""Get insert SQL query for specific resource type. If Resource base class method is called, then resource not supported yet

		   Supported resource will implement this function

		Returns:
			Optional[str]: SQL insert command query for respective resource type
		"""
		if self.isVirtual():
			return self._getInsertGeneralQuery()

		return None


	def getUpdateQuery(self) -> Optional[str]:
		"""Get update SQL query

		Returns:
			Optional[str]: SQL update command query for respective resource type
		"""
		colResource = ""
		colType = ""
		tyShortName = self.tpe.split(":")[1]
  
		# Build query for SET column for each modified attribute
		for key, value in self.dict.items():
			if key in self._excludeFromDBUpdate:
				continue
			# Seperate 
			if (key in self.universalCommonAttributes) or (key in self.internalAttributes):
				colResource = colResource + f",{key}={self.validateAttributeValue(value)}"
			else:
				colType = colType + f",{key}={self.validateAttributeValue(value)}"

		# Remove first comma from string
		colResource = colResource[1:]
		colType = colType[1:]

		# Build query by checking if there are attributes that not in resource table (universal/common attributes)
		query = None
		if colType == "":
			query = f"UPDATE resources SET {colResource} WHERE ri = '{self.ri}'"
		elif colResource == "" and colType != "":
			query = f"""
					WITH resource_table AS (
						SELECT index, ri FROM resources WHERE ri = '{self.ri}'
					)
					UPDATE {tyShortName} SET {colType} FROM resource_table WHERE {tyShortName}.resource_index = resource_table.index;
					"""
		elif colType != "":
			query = f"""
					WITH resource_table AS (
						UPDATE resources SET {colResource} WHERE ri = '{self.ri}'
						RETURNING index
					)
					UPDATE {tyShortName} SET {colType} FROM resource_table WHERE {tyShortName}.resource_index = resource_table.index;
					"""
   
		return query
     

	#########################################################################
	#
	#	Misc utilities
	#

	def __str__(self) -> str:
		""" String representation of the resource's attributes.

			Return:
				String with the resource formatted as a JSON structure
		"""
		return str(self.asDict())


	def __repr__(self) -> str:
		""" Object representation as string.

			Return:
				String that identifies the resource.
		"""
		return f'{self.tpe}(ri={self.ri}, srn={self.getSrn()})'


	def __eq__(self, other:object) -> bool:
		"""	Test for equality of the resource to another resource.

			Args:
				other: Other object to test for.
			Return:
				If the *other* object is a Resource instance and has the same resource ID, then *True* is returned, of *False* otherwise.
		"""
		return isinstance(other, Resource) and self.ri == other.ri


	def isModifiedAfter(self, otherResource:Resource) -> bool:
		"""	Test whether this resource has been modified after another resource.

			Args:
				otherResource: Another resource used for the test.
			Return:
				True if this resource has been modified after *otherResource*.
		"""
		return str(self.lt) > str(otherResource.lt)


	def retrieveParentResource(self) -> Resource:
		"""	Retrieve the parent resource of this resouce.

			Return:
				The parent Resource of the resource.
		"""
		return CSE.dispatcher.retrieveLocalResource(self.pi).resource	#type:ignore[no-any-return]


	def retrieveParentResourceRaw(self) -> JSON:
		"""	Retrieve the raw (!) parent resource of this resouce.

			Return:
				Document of the parent resource
		"""
		return CSE.storage.retrieveResourceRaw(self.pi).resource


	def getOriginator(self) -> str:
		"""	Retrieve a resource's originator.

			Return:
				The resource's originator.
		"""
		return self[self._originator]
	

	def setOriginator(self, originator:str) -> None:
		"""	Set a resource's originator.

			This is the originator that created the resource. It is stored internally within the resource.

			Args:
				originator: The originator to assign to a resource.
		"""
		self.setAttribute(self._originator, originator, overwrite = True)
	


	def getAnnouncedTo(self) -> list[Tuple[str, str]]:
		"""	Return the internal *announcedTo* list attribute of a resource.

			Return:
				The internal list of *announcedTo* tupples (csi, remote resource ID) for this resource.
		"""
		return self[self._announcedTo]

	
	def setResourceName(self, rn:str) -> None:
		"""	Set the resource name. 
		
			Also set/update the internal structured resource name.
			
			Args:
				rn: The new resource name for the resource.
		"""
		self.setAttribute('rn', rn)

		# determine and add the srn, only when this is a local resource, otherwise we don't need this information
		# It is *not* a remote resource when the __remoteID__ is set
		if not self[self._remoteID]:
			self.setSrn(Utils.structuredPath(self))


	def getSrn(self) -> str:
		"""	Retrieve a resource's full structured resource name.

			Return:
				The resource's full structured resource name.
		"""
		return self[self._srn]
	

	def setSrn(self, srn:str) -> None:
		"""	Set a resource's full structured resource name.

			Args:
				srn: The full structured resource name to assign to a resource.
		"""
		self.setAttribute(self._srn, srn, overwrite = True)


	def getRVI(self) -> str:
		"""	Retrieve a resource's release version indicator.

			Return:
				The resource's *rvi*.
		"""
		return self[self._rvi]
	

	def setRVI(self, rvi:str) -> None:
		"""	Assign the release version for a resource.

			This is usually assigned from the *rvi* indicator in the resource's CREATE request.

			Args:
				rvi: Original CREATE request's *rvi*.
		"""
		self.setAttribute(self._rvi, rvi, overwrite = True)