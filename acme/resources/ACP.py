#
#	ACP.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
""" AccessControlPolicy (ACP) resource type """

from __future__ import annotations
from typing import List, Optional

from ..helpers.TextTools import simpleMatch
from ..etc import Utils
from ..etc.Types import AttributePolicyDict, ResourceTypes, Result, Permission, JSON
from ..services import CSE
from ..services.Logging import Logging as L
from ..resources.Resource import Resource
from ..resources.AnnounceableResource import AnnounceableResource


class ACP(AnnounceableResource):
	""" AccessControlPolicy (ACP) resource type """

	_allowedChildResourceTypes:list[ResourceTypes] = [ ResourceTypes.SUB ] # TODO Transaction to be added
	""" The allowed child-resource types. """

	# Assigned during startup in the Importer.
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
			'at': None,
			'aa': None,
			'ast': None,

			# Resource attributes
			'pv': None,
			'pvs': None,
			'adri': None,
			'apri': None,
			'airi': None
	}
	"""	Attributes and `AttributePolicy` for this resource type. """


	def __init__(self, dct:JSON, 
					   pi:Optional[str] = None, 
					   rn:Optional[str] = None, 
					   create:Optional[bool] = False) -> None:
		super().__init__(ResourceTypes.ACP, dct, pi, create = create, inheritACP = True, rn = rn)

		self.setAttribute('pv/acr', [], overwrite = False)
		self.setAttribute('pvs/acr', [], overwrite = False)


	def validate(self, originator:Optional[str] = None, 
					   create:Optional[bool] = False, 
					   dct:Optional[JSON] = None, 
					   parentResource:Optional[Resource] = None) -> Result:
		# Inherited
		if not (res := super().validate(originator, create, dct, parentResource)).status:
			return res
		
		if dct and (pvs := Utils.findXPath(dct, f'{ResourceTypes.ACPAnnc.tpe()}/pvs')):
			if len(pvs) == 0:
				return Result.errorResult(dbg = 'pvs must not be empty')
		if not self.pvs:
			return Result.errorResult(dbg = 'pvs must not be empty')

		# Check acod
		def _checkAcod(acrs:list) -> Result:
			if not acrs:
				return Result.successResult()
			for acr in acrs:
				if (acod := acr.get('acod')):
					for each in acod:
						if not (chty := each.get('chty')) or not isinstance(chty, list):
							return Result.errorResult(dbg = 'chty is mandatory in acod')
			return Result.successResult()

		if not (res := _checkAcod(Utils.findXPath(dct, f'{ResourceTypes.ACPAnnc.tpe()}/pv/acr'))).status:
			return res
		if not (res := _checkAcod(Utils.findXPath(dct, f'{ResourceTypes.ACPAnnc.tpe()}/pvs/acr'))).status:
			return res

		return Result.successResult()


	def deactivate(self, originator:str) -> None:
		# Inherited
		super().deactivate(originator)
  
		# NOTE: [IMPROVEMENT] update doesn't have to create resource then update. Which in this case only need ri and acpi, the only update acpi

		# Remove own resourceID from all acpi
		L.isDebug and L.logDebug(f'Removing acp.ri: {self.ri} from assigned resource acpi')    
		for r in CSE.storage.retrieveResourceBy(acpi = self.ri):	# search for presence in acpi, not perfect match
			acpi = r.acpi
			acpi.remove(self.ri)
			r['acpi'] = acpi if len(acpi) > 0 else None	# Remove acpi from resource if empty
			r.dbUpdate()


	def validateAnnouncedDict(self, dct:JSON) -> JSON:
		# Inherited
		if acr := Utils.findXPath(dct, f'{ResourceTypes.ACPAnnc.tpe()}/pvs/acr'):
			acr.append( { 'acor': [ CSE.cseCsi ], 'acop': Permission.ALL } )
		return dct


	#########################################################################
	#
	#	Resource specific
	#

	#	Permission handlings

	def addPermission(self, originators:list[str], permission:Permission) -> None:
		"""	Add new general permissions to the ACP resource.
		
			Args:
				originators: List of originator identifiers.
				permission: Bit-field of oneM2M request permissions
		"""
		o = list(set(originators))	# Remove duplicates from list of originators
		if p := self['pv/acr']:
			p.append({'acop' : permission, 'acor': o})


	def removePermissionForOriginator(self, originator:str) -> None:
		"""	Remove the permissions for an originator.
		
			Args:
				originator: The originator for to remove the permissions.
		"""
		if p := self['pv/acr']:
			for acr in p:
				if originator in acr['acor']:
					p.remove(acr)
					

	def addSelfPermission(self, originators:List[str], permission:Permission) -> None:
		"""	Add new **self** - permissions to the ACP resource.
		
			Args:
				originators: List of originator identifiers.
				permission: Bit-field of oneM2M request permissions
		"""
		if p := self['pvs/acr']:
			p.append({'acop' : permission, 'acor': list(set(originators))}) 	# list(set()) : Remove duplicates from list of originators


	def checkPermission(self, originator:str, requestedPermission:Permission, ty:ResourceTypes) -> bool:
		"""	Check whether an *originator* has the requested permissions.

			Args:
				originator: The originator to test the permissions for.
				requestedPermission: The permissions to test.
				ty: If the resource type is given then it is checked for CREATE (as an allowed child resource type), otherwise as an allowed resource type.
			Return:
				If any of the configured *accessControlRules* of the ACP resource matches, then the originatorhas access, and *True* is returned, or *False* otherwise.
		"""
		# L.isDebug and L.logDebug(f'originator: {originator} requestedPermission: {requestedPermission}')
		for acr in self['pv/acr']:
			# L.isDebug and L.logDebug(f'p.acor: {p['acor']} requestedPermission: {p['acop']}')

			# Check Permission-to-check first
			if requestedPermission & acr['acop'] == Permission.NONE:	# permission not fitting at all
				continue

			# Check acod : chty
			if acod := acr.get('acod'):
				for eachAcod in acod:
					if requestedPermission == Permission.CREATE:
						if ty is None or ty not in eachAcod.get('chty'):	# ty is an int
							continue										# for CREATE: type not in chty
					else:
						if ty is not None and ty != eachAcod.get('ty'):
							continue								# any other Permission type: ty not in chty
					break # found one, so apply the next checks further down
				else:
					continue	# NOT found, so continue the overall search

				# TODO support acod/specialization


			# Check originator
			if 'all' in acr['acor'] or requestedPermission == Permission.NOTIFY:
				return True		
			for acr in acr['acor']:
				# if acor is a groupId, check from resourceId prefix
				if acr[:3] == "grp":
					try:
						if not (grp := CSE.dispatcher.retrieveResource(acr).resource):
							L.isDebug and L.logDebug(f'Group resource not found: {acr}')
							# TODO: If not found, delete groupId from acp acor
							continue

						if originator in grp.mid:
							L.isDebug and L.logDebug(f'Originator found in group member')
							return True
					except Exception as e:
						L.logErr(f'GRP resource not found for ACP check: {acr}')
						continue # Not much that we can do here

				# Check string match with pattern
				if simpleMatch(originator, acr):
					return True

		return False


	def checkSelfPermission(self, originator:str, requestedPermission:Permission) -> bool:
		"""	Check whether an *originator* has the requested permissions to the `ACP` resource itself.

			Args:
				originator: The originator to test the permissions for.
				requestedPermission: The permissions to test.
			Return:
				If any of the configured *accessControlRules* of the ACP resource matches, then the originatorhas access, and *True* is returned, or *False* otherwise.
		"""
		# NOTE The same function also exists in ACPAnnc.py

		for p in self['pvs/acr']:
			if requestedPermission & p['acop'] == 0:	# permission not fitting at all
				continue
			# TODO check acod in pvs
			if 'all' in p['acor'] or originator in p['acor']:
				return True
			if any([ simpleMatch(originator, a) for a in p['acor'] ]):	# check whether there is a wildcard match
				return True
		return False


	#	Databases Related

	def getInsertQuery(self) -> Optional[str]:
		query = """
					INSERT INTO public.acp(resource_index, pv, pvs, adri, apri, airi)
					SELECT rt.index, {}, {}, {}, {}, {} FROM resource_table rt;
				"""

		return self._getInsertGeneralQuery() + query.format(
			self.validateAttributeValue(self['pv']),
			self.validateAttributeValue(self['pvs']),
			self.validateAttributeValue(self['adri']),
			self.validateAttributeValue(self['apri']),
			self.validateAttributeValue(self['airi'])
		)