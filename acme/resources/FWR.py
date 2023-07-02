#
#	FWR.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ResourceType: mgmtObj:Firmware
#

from __future__ import annotations
from typing import Optional

from ..etc.Types import AttributePolicyDict, ResourceTypes, JSON
from ..resources.MgmtObj import MgmtObj


statusUninitialized = 0
statusSuccessful = 1
statusFailure = 2
statusInProcess = 3

defaultFirmwareName = 'unknown'
defaultVersion = '0.0'
defaultURL = 'unknown'
defaultUDS = { 'acn' : '', 'sus' : statusUninitialized }


class FWR(MgmtObj):

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
		
		# MgmtObj attributes
		'mgd': None,
		'obis': None,
		'obps': None,
		'dc': None,
		'mgs': None,
		'cmlk': None,

		# Resource attributes
		'vr': None,
		'fwn': None,
		'url': None,
		'uds': None,
		'ud': None
	}


	def __init__(self, dct:Optional[JSON] = None, 
					   pi:Optional[str] = None, 
					   create:Optional[bool] = False) -> None:
		super().__init__(dct, pi, mgd = ResourceTypes.FWR, create = create)

		self.setAttribute('vr', defaultVersion, overwrite = False)
		self.setAttribute('fwn', defaultFirmwareName, overwrite = False)
		self.setAttribute('url', defaultURL, overwrite = False)
		self.setAttribute('uds', defaultUDS, overwrite = False)
		self.setAttribute('ud', False, overwrite = False)


	#########################################################################
	#
	#	Resource specific
	#

	#	Database Related

	def getInsertQuery(self) -> Optional[str]:
		query = """
					INSERT INTO public.fwr(resource_index, mgd, obis, obps, dc, mgs, cmlk, vr, fwn, url, ud, uds)
					SELECT rt.index, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} FROM resource_table rt;
				"""

		return self._getInsertGeneralQuery() + query.format(
			self.validateAttributeValue(self['mgd']),
			self.validateAttributeValue(self['obis']),
			self.validateAttributeValue(self['obps']),
			self.validateAttributeValue(self['dc']),
			self.validateAttributeValue(self['mgs']),
			self.validateAttributeValue(self['cmlk']),
			self.validateAttributeValue(self['vr']),
			self.validateAttributeValue(self['fwn']),
			self.validateAttributeValue(self['url']),
			self.validateAttributeValue(self['ud']),
			self.validateAttributeValue(self['uds'])
		)
