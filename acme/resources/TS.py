#
#	TS.py
#
#	(c) 2021 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ResourceType: TimeSeries
#

from __future__ import annotations
from typing import Optional

from ..etc.Types import AttributePolicyDict, ResourceTypes, Result, ResponseStatusCode, JSON
from ..etc import Utils, DateUtils
from ..services.Configuration import Configuration
from ..services import CSE
from ..services.Logging import Logging as L
from ..resources.Resource import Resource
from ..resources.AnnounceableResource import AnnounceableResource
from ..resources import Factory


# CSE default:
#	- peid is set to pei/2 if ommitted, and pei is set

class TS(AnnounceableResource):

	# Specify the allowed child-resource types
	_allowedChildResourceTypes = [ ResourceTypes.ACTR, 
								   ResourceTypes.TSI, 
								   ResourceTypes.SMD, 
								   ResourceTypes.SUB ]

	# Attributes and Attribute policies for this Resource Class
	# Assigned during startup in the Importer
	_attributes:AttributePolicyDict = {		
		# Common and universal attributes
		'rn': None,
		'ty': None,
		'ri': None,
		'pi': None,
		'ct': None,
		'lt': None,
		'et': None,
		'lbl': None,
		'cstn': None,
		'acpi':None,
		'at': None,
		'aa': None,
		'ast': None,
		'daci': None,
		'cr': None,
		'loc': None,

		# Resource attributes
		'mni': None,
		'mbs': None,
		'mia': None,
		'cni': None,
		'cbs': None,
		'pei': None,
		'peid': None,
		'mdd': None,
		'mdn': None,
		'mdlt': None,
		'mdc': None,
		'mdt': None,
		'cnf': None,
		'or': None
	}


	def __init__(self, dct:Optional[JSON] = None, 
					   pi:Optional[str] = None, 
					   create:Optional[bool] = False) -> None:
		super().__init__(ResourceTypes.TS, dct, pi, create = create)

		self.setAttribute('mdd', False, overwrite = False)	# Default is False if not provided
		self.setAttribute('cni', 0, overwrite = False)
		self.setAttribute('cbs', 0, overwrite = False)
		if Configuration.get('cse.ts.enableLimits'):	# Only when limits are enabled
			self.setAttribute('mni', Configuration.get('cse.ts.mni'), overwrite = False)
			self.setAttribute('mbs', Configuration.get('cse.ts.mbs'), overwrite = False)
			self.setAttribute('mdn', Configuration.get('cse.ts.mdn'), overwrite = False)

		self.__validating = False	# semaphore for validating


	def activate(self, parentResource:Resource, originator:str) -> Result:
		if not (res := super().activate(parentResource, originator)).status:
			return res
		# Validation of CREATE is done in self.validate()

		# register latest and oldest virtual resources
		L.isDebug and L.logDebug(f'Registering latest and oldest virtual resources for: {self.ri}')

		# add latest
		resource = Factory.resourceFromDict({}, pi = self.ri, ty = ResourceTypes.TS_LA).resource	# rn is assigned by resource itself
		if not (res := CSE.dispatcher.createLocalResource(resource)).resource:
			return Result.errorResult(rsc = res.rsc, dbg = res.dbg)

		# add oldest
		resource = Factory.resourceFromDict({}, pi = self.ri, ty = ResourceTypes.TS_OL).resource	# rn is assigned by resource itself
		if not (res := CSE.dispatcher.createLocalResource(resource)).resource:
			return Result.errorResult(rsc = res.rsc, dbg = res.dbg)
		
		self._validateDataDetect()
		return Result.successResult()


	def deactivate(self, originator:str) -> None:
		super().deactivate(originator)
		CSE.timeSeries.stopMonitoringTimeSeries(self.ri)


	def update(self, dct:Optional[JSON] = None, 
					 originator:Optional[str] = None, 
					 doValidateAttributes:Optional[bool] = True) -> Result:

		# Extra checks if mdd is present in an update
		updatedAttributes = Utils.findXPath(dct, 'm2m:ts')
		
		if (mddNew := updatedAttributes.get('mdd')) is not None:	# boolean
			# Check that mdd is updated alone
			if any(key in ['mdt', 'mdn', 'peid', 'pei'] for key in updatedAttributes.keys()):
				return Result.errorResult(dbg = L.logDebug('mdd must not be updated together with mdt, mdn, pei or peid.'))

			# Clear the list if mddNew is deliberatly set to True
			if mddNew == True:
				self._clearMdlt()
				# Restart the monitoring process
				# The actual "restart" is happening when the next TSI is received
				L.isDebug and L.logDebug(f'(Re)Start monitoring <TS>: {self.ri}. Actual monitoring begins when first <TSI> is received.')
				CSE.timeSeries.pauseMonitoringTimeSeries(self.ri)
			else:
				L.isDebug and L.logDebug(f'Pause monitoring <TS>: {self.ri}')
				CSE.timeSeries.pauseMonitoringTimeSeries(self.ri)
		
		# Check that certain attributes are not updated when mdd is true
		if self.mdd  == True: # existing mdd
			if any(key in ['mdt', 'mdn', 'peid', 'pei'] for key in updatedAttributes.keys()):
				return Result.errorResult(dbg = L.logDebug('mdd must not be True when mdt, mdn, pei or peid are updated.'))

		if (peiNew := updatedAttributes.get('pei')) is not None: # integer
			if 'peid' not in updatedAttributes:
				# Add peid if not provided to new attributes, real update below (!)
				updatedAttributes['peid'] = int(peiNew / 2)	# CSE internal policy
		# further pei / peid checks are done in validate()


		if mddNew == True or (mddNew is None and self.mdd == True):	# either mdd is set to True or was and stays True
			mdt = updatedAttributes.get('mdt', self.mdt) 		# old or new value,  default: self.mdt
			peid = updatedAttributes.get('peid', self.peid)		# old or new value, default: self.peid
			if mdt is not None and peid is not None and mdt <= peid:
				return Result.errorResult(dbg = L.logDebug('mdt must be > peid'))

		# Check if mdn was changed and shorten mdlt accordingly, if exists
		# shorten the mdlt if a limit is set in mdn
		# if (mdnNew := updatedAttributes.get('mdn') ) is not None:	# integer
		# 	if (mdlt := cast(list, self.mdlt)) and (l := len(mdlt)) > mdnNew:
		# 		mdlt = mdlt[l - mdnNew:]
		# 		self['mdlt'] = mdlt

		# Check if mdt was changed in an update
		# if 'mdt' in updatedAttributes:	# mdt is in the update, either True, False or None!
		# 	mdtNew = updatedAttributes.get('mdt')
		# 	if mdtNew is None and CSE.timeSeries.isMonitored(self.ri):	# it is in the update, but set to None, meaning remove the mdt from the TS
		# 		CSE.timeSeries.stopMonitoringTimeSeries(self.ri)

		# Do real update last
		if not (res := super().update(dct, originator)).status:
			return res
		
		self._validateChildren()	# Check consequences from the update
		self._validateDataDetect(updatedAttributes)

		return Result.successResult()

 
	def validate(self, originator:Optional[str] = None, 
					   create:Optional[bool] = False, 
					   dct:Optional[JSON] = None, 
					   parentResource:Optional[Resource] = None) -> Result:
		L.isDebug and L.logDebug(f'Validating timeSeries: {self.ri}')
		if (res := super().validate(originator, create, dct, parentResource)).status == False:
			return res
		
		# Check the format of the CNF attribute
		if cnf := self.cnf:
			if not (res := CSE.validator.validateCNF(cnf)).status:
				return Result.errorResult(dbg = res.dbg)

		# Check for peid
		if self.peid is not None and self.pei is not None:	# pei(d) is an int
			if not self.peid <= self.pei/2:	# complicated, but reflects the text in the spec
				return Result.errorResult(dbg = L.logDebug('peid must be <= pei/2'))
		elif self.pei is not None:	# pei is an int
			self.setAttribute('peid', int(self.pei/2), False)	# CSE internal policy
		
		# Checks for MDT
		if self.mdd: # present and True
			# Add mdlt and mdc, if not already added ( No overwrite !)
			self._clearMdlt(False)
			if self.mdt is None:
				return Result.errorResult(dbg = L.logDebug('mdt must be set if mdd is True'))
			if self.mdt is not None and self.peid is not None and self.mdt <= self.peid:
				return Result.errorResult(dbg = L.logDebug('mdt must be > peid'))
		
		self._validateChildren()
		return Result.successResult()


	def childWillBeAdded(self, childResource:Resource, originator:str) -> Result:
		if not (res := super().childWillBeAdded(childResource, originator)).status:
			return res
		
		# Check whether the child's rn is "ol" or "la".
		if (rn := childResource['rn']) and rn in ['ol', 'la']:
			return Result.errorResult(rsc = ResponseStatusCode.operationNotAllowed, dbg = 'resource types "latest" or "oldest" cannot be added')
	
		# Check whether the size of the TSI doesn't exceed the mbs
		if childResource.ty == ResourceTypes.TSI and self.mbs is not None:					# mbs is an int
			if childResource.cs is not None and childResource.cs > self.mbs:	# cs is an int
				return Result.errorResult(rsc = ResponseStatusCode.notAcceptable, dbg = 'child content sizes would exceed mbs')

		# Check whether another TSI has the same dgt value set
		# tsis = CSE.storage.searchByFragment({ 	'ty'	: ResourceTypes.TSI,
		# 										'pi'	: self.ri,
		# 										'dgt'	: childResource.dgt
		# })
		#! Not supported
		tsis = []
		if len(tsis) > 0:	# Error if yes
			return Result.errorResult(rsc = ResponseStatusCode.conflict, dbg = f'timeSeriesInstance with the same dgt: {childResource.dgt} already exists')

		return Result.successResult()


	# Handle the addition of new TSI. Basically, get rid of old ones.
	def childAdded(self, childResource:Resource, originator:str) -> None:
		L.isDebug and L.logDebug(f'Child resource added: {childResource.ri}')
		super().childAdded(childResource, originator)
		if childResource.ty == ResourceTypes.TSI:	# Validate if child is TSI

			# Check for mia handling. This sets the et attribute in the TSI
			if self.mia is not None:
				# Take either mia or the maxExpirationDelta, whatever is smaller
				maxEt = DateUtils.getResourceDate(self.mia 
												  if self.mia <= CSE.request.maxExpirationDelta 
												  else CSE.request.maxExpirationDelta)
				# Only replace the childresource's et if it is greater than the calculated maxEt
				if childResource.et > maxEt:
					childResource.setAttribute('et', maxEt)
					childResource.dbUpdate()

			self.validate(originator)	# Handle old TSI removals
		
			# Add to monitoring if this is enabled for this TS (mdd & pei & mdt are not None, and mdd==True)
			if self.mdd and self.pei is not None and self.mdt is not None:
				CSE.timeSeries.updateTimeSeries(self, childResource)
		
		elif childResource.ty == ResourceTypes.SUB:		# start monitoring
			if childResource['enc/md']:
				CSE.timeSeries.addSubscription(self, childResource)


	# Handle the removal of a TSI. 
	def childRemoved(self, childResource:Resource, originator:str) -> None:
		L.isDebug and L.logDebug(f'Child resource removed: {childResource.ri}')
		super().childRemoved(childResource, originator)
		if childResource.ty == ResourceTypes.TSI:	# Validate if child was TSI
			self._validateChildren()
		elif childResource.ty == ResourceTypes.SUB:
			if childResource['enc/md']:
				CSE.timeSeries.removeSubscription(self, childResource)


	# handle eventuel updates of subscriptions
	def childUpdated(self, childResource:Resource, updatedAttributes:JSON, originator:str) -> None:
		super().childUpdated(childResource, updatedAttributes, originator)
		if childResource.ty == ResourceTypes.SUB and childResource['enc/md']:
			CSE.timeSeries.updateSubscription(self, childResource)		


	def _validateChildren(self) -> None:
		""" Internal validation and checks. This called more often then just from
			the validate() method.
		"""
		# Check whether we already are in validation the children (ie prevent unfortunate recursion by the Dispatcher)
		if self.__validating:
			return
		self.__validating = True

		# TODO Optimize: Do we really need the resources?
		tsis = self.timeSeriesInstances()	# retrieve TIS child resources
		cni = len(tsis)			
			
		# Check number of instances
		if (mni := self.mni) is not None:	# mni is an int
			while cni > mni and cni > 0:
				L.isDebug and L.logDebug(f'cni > mni: Removing <tsi>: {tsis[0].ri}')
				# remove oldest
				# Deleting a child must not cause a notification for 'deleteDirectChild'.
				# Don't do a delete check means that TS.childRemoved() is not called, where subscriptions for 'deleteDirectChild'  is tested.
				CSE.dispatcher.deleteLocalResource(tsis[0], parentResource = self, doDeleteCheck = False)
				del tsis[0]
				cni -= 1	# decrement cni when deleting a <cin>

		# Calculate cbs
		cbs = sum([ each.cs for each in tsis])

		# check size
		if (mbs := self.mbs) is not None:
			while cbs > mbs and cbs > 0:
				L.isDebug and L.logDebug(f'cbs > mbs: Removing <tsi>: {tsis[0].ri}')
				# remove oldest
				cbs -= tsis[0]['cs']
				# Deleting a child must not cause a notification for 'deleteDirectChild'.
				# Don't do a delete check means that TS.childRemoved() is not called, where subscriptions for 'deleteDirectChild'  is tested.
				CSE.dispatcher.deleteLocalResource(tsis[0], parentResource = self, doDeleteCheck = False)
				del tsis[0]
				cni -= 1	# decrement cni when deleting a <tsi>

		# Some attributes may have been updated, so store the resource 
		self['cni'] = cni
		self['cbs'] = cbs
		self.dbUpdate()
	
		# End validating
		self.__validating = False


	def _validateDataDetect(self, updatedAttributes:Optional[JSON] = None) -> None:
		"""	This method checks and enables or disables certain data detect monitoring attributes.
		"""
		L.isDebug and L.logDebug('Validating data detection')

		mdd = self.mdd
		
		# If any of mdd, pei or mdt becomes None, or is mdd==False, then stop monitoring this TS
		if not mdd or not self.pei or not self.mdt:
			if CSE.timeSeries.isMonitored(self.ri):
				CSE.timeSeries.pauseMonitoringTimeSeries(self.ri)


        # If any parameters related to the missing data detection process (missingDataDetectTimer, missingDataMaxNr,
        # periodicIntervalDelta, periodicInterval) are updated while the data detection process is paused the Hosting CSE
        # will clear the missingDataList and missingDataCurrentNr.
		if self.mdd  == False:
			if updatedAttributes and any(key in ['mdt', 'mdn', 'peid', 'pei'] for key in updatedAttributes.keys()):
				self._clearMdlt()

		
		# # Check if mdn was changed and shorten mdlt accordingly, if exists
		# if updatedAttributes:
		# 	# shorten the mdlt if a limit is set in mdn
		# 	if self.mdlt and (newMdn := updatedAttributes.get('mdn') ) is not None:	# Returns None if dct is None or not found in dct
		# 		mdlt = self.mdlt
		# 		if (l := len(mdlt)) > newMdn:
		# 			mdlt = mdlt[l-newMdn:]
		# 			self['mdlt'] = mdlt

		# 	# Check if mdt was changed in an update
		# 	if (newMdt := updatedAttributes.get('mdt')):	# mdt is in the update, either True, False or None!
		# 		if newMdt is None and CSE.timeSeries.isMonitored(self.ri):				# it is in the update, but set to None, meaning remove the mdt from the TS
		# 			CSE.timeSeries.stopMonitoringTimeSeries(self.ri)

		# Always set the mdc to the length of mdlt
		if self.hasAttribute('mdlt'):
			self.setAttribute('mdc', len(self.mdlt))

		# Save changes
		self.dbUpdate()


	def _clearMdlt(self, overwrite:Optional[bool] = True) -> None:
		"""	Clear the missingDataList and missingDataCurrentNr attributes.

			Args:
				overwrite: Force overwrite.
		"""
		self.setAttribute('mdlt', [], overwrite)
		self.setAttribute('mdc', 0, overwrite)


	def timeSeriesInstances(self) -> list[Resource]:
		"""	Get all timeSeriesInstances of a timeSeries and return a sorted (by ct) list
		""" 
		return sorted(CSE.dispatcher.directChildResources(self.ri, ResourceTypes.TSI), key = lambda x: x.ct) # type:ignore[no-any-return]


	def addDgtToMdlt(self, dgtToAdd:float) -> None:
		"""	Add a dataGenerationTime *dgtToAdd* to the mdlt of this resource.
		"""
		self._clearMdlt(False)												# Add to mdlt, just in case it hasn't created before
		self.mdlt.append(DateUtils.toISO8601Date(dgtToAdd))					# Add missing dgt to TS.mdlt
		if (mdn := self.mdn) is not None:									# mdn may not be set. Then this list grows forever
			if len(self.mdlt) > mdn:										# If mdlt is bigger then mdn allows
				self.setAttribute('mdlt', self.mdlt[1:], overwrite = True)	# Shorten the mdlt
			self.setAttribute('mdc', len(self.mdlt), overwrite = True)		# Set the mdc
			self.dbUpdate()													# Update in DB

