#
#	AE.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
""" Application Entity (AE) resource type """

from __future__ import annotations
from typing import Optional

from ..etc.Types import AttributePolicyDict, ResourceTypes, ContentSerializationType, Result, ResponseStatusCode, JSON
from ..etc.Utils import uniqueAEI
from ..services.Logging import Logging as L
from ..services import CSE
from ..resources.Resource import Resource
from ..resources.AnnounceableResource import AnnounceableResource


class AE(AnnounceableResource):
	""" Application Entity (AE) resource type """

	_allowedChildResourceTypes:list[ResourceTypes] = [ ResourceTypes.ACP,
													   ResourceTypes.ACTR,
													   ResourceTypes.CNT,
													   ResourceTypes.CRS,
													   ResourceTypes.FCNT,
													   ResourceTypes.GRP,
													   ResourceTypes.PCH,
													   ResourceTypes.SMD,
													   ResourceTypes.SUB,
													   ResourceTypes.TS,
													   ResourceTypes.TSB ]
	""" The allowed child-resource types. """

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
			'daci': None,
			'ast': None,
			'loc': None,	

			# Resource attributes
			'apn': None,
			'api': None,
			'aei': None,
			'poa': None,
			'nl': None,
			'rr': None,
			'csz': None,
			'esi': None,
			'mei': None,
			'srv': None,
			'regs': None,
			'trps': None,
			'scp': None,
			'tren': None,
			'ape': None,
			'or': None,
	}
	"""	Attributes and `AttributePolicy` for this resource type. """


	def __init__(self, dct:Optional[JSON] = None, 
					   pi:Optional[str] = None, 
					   create:Optional[bool] = False) -> None:
		super().__init__(ResourceTypes.AE, dct, pi, create = create)

		self.setAttribute('aei', uniqueAEI(), overwrite = False)
		self.setAttribute('rr', False, overwrite = False)


	def childWillBeAdded(self, childResource:Resource, originator:str) -> Result:
		# Inherited
		if not (res := super().childWillBeAdded(childResource, originator)).status:
			return res

		# Perform checks for <PCH>	
		if childResource.ty == ResourceTypes.PCH:
			# Check correct originator. Even the ADMIN is not allowed that		
			if self.aei != originator:
				L.logDebug(dbg := f'Originator must be the parent <AE>')
				return Result.errorResult(rsc = ResponseStatusCode.originatorHasNoPrivilege, dbg = dbg)

			# check that there will only by one PCH as a child
			if CSE.dispatcher.countDirectChildResources(self.ri, ty = ResourceTypes.PCH) > 0:
				return Result.errorResult(dbg = 'Only one PCH per AE is allowed')

		return Result.successResult()


	def validate(self, originator:Optional[str] = None,
					   create:Optional[bool] = False, 
					   dct:Optional[JSON] = None, 
					   parentResource:Optional[Resource] = None) -> Result:
		# Inherited
		if not (res := super().validate(originator, create, dct, parentResource)).status:
			return res

		self._normalizeURIAttribute('poa')

		# Update the nl attribute in the hosting node (similar to csebase) in case 
		# the AE is now on a different node. This shouldn't be happen in reality,
		# but technically it is allowed.
		nl = self.nl
		_nl_ = self.__node__
		if nl or _nl_:
			if nl != _nl_:	# if different node
				ri = self.ri

				# Remove from old node first
				if _nl_:
					self._removeAEfromNOD(_nl_)
				self[Resource._node] = nl

				# Add to new node
				if node := CSE.dispatcher.retrieveResource(nl).resource:	# new node
					if not (hael := node.hael):
						node['hael'] = [ ri ]
					else:
						if isinstance(hael, list):
							hael.append(ri)
							node['hael'] = hael
					node.dbUpdate()
			self[Resource._node] = nl
		
		# check csz attribute
		if csz := self.csz:
			for c in csz:
				if c not in ContentSerializationType.supportedContentSerializations():
					return Result.errorResult(dbg  = 'unsupported content serialization: {c}')
		
		# check api attribute
		if not (api := self['api']) or len(api) < 2:	# at least R|N + another char
			return Result.errorResult(dbg = 'missing or empty attribute: "api"')
		if api.startswith('N'):
			pass # simple format
		elif api.startswith('R'):
			if len(api.split('.')) < 3:
				return Result.errorResult(dbg = 'wrong format for registered ID in attribute "api": to few elements')

		# api must normally begin with a lower-case "r", but it is allowed for release 2a and 3
		elif api.startswith('r'):
			if (rvi := self.getRVI()) is not None and rvi not in ['2a', '3']:
				return Result.errorResult(dbg = L.logWarn('lower case "r" is only allowed for release versions "2a" and "3"'))
		else:
			return Result.errorResult(dbg = L.logWarn(f'wrong format for ID in attribute "api": {api} (must start with "R" or "N")'))

		return Result.successResult()


	def deactivate(self, originator:str) -> None:
		# Inherited
		super().deactivate(originator)

		# Remove itself from the node link in a hosting <node>
		if nl := self.nl:
			self._removeAEfromNOD(nl)


	#########################################################################
	#
	#	Resource specific
	#

	def _removeAEfromNOD(self, nodeRi:str) -> None:
		""" Remove AE from hosting Node. 

			Args:
				nodeRi: The hosting node's resource ID.
		"""
		ri = self.ri
		if node := CSE.dispatcher.retrieveResource(nodeRi).resource:
			if (hael := node.hael) and isinstance(hael, list) and ri in hael:
				hael.remove(ri)
				if len(hael) == 0:
					node.delAttribute('hael')
				else:
					node['hael'] = hael
				node.dbUpdate()

	
	#	Databases Related
	
	def getInsertQuery(self) -> Optional[str]:
		query = """
					INSERT INTO public.ae(resource_index, apn, api, aei, mei, tri, trn, poa, regs, trps, ontologyref, rr, nl, csz, scp, srv)
					SELECT rt.index, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} FROM resource_table rt;
				"""

		return self._getInsertGeneralQuery() + query.format(
			self.validateAttributeValue(self['apn']),
			self.validateAttributeValue(self['api']),
			self.validateAttributeValue(self['aei']),
			self.validateAttributeValue(self['mei']),
			self.validateAttributeValue(self['tri']),
			self.validateAttributeValue(self['trn']),
			self.validateAttributeValue(self['poa']),
			self.validateAttributeValue(self['regs']),
			self.validateAttributeValue(self['trps']),
			self.validateAttributeValue(self['or']),
			self.validateAttributeValue(self['rr']),
			self.validateAttributeValue(self['nl']),
			self.validateAttributeValue(self['csz']),
			self.validateAttributeValue(self['scp']),
			self.validateAttributeValue(self['srv'])
		)


