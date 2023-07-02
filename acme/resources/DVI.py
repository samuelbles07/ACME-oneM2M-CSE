#
#	DVI.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ResourceType: mgmtObj:DeviceInfo
#

from __future__ import annotations
from typing import Optional

from ..etc.Types import AttributePolicyDict, ResourceTypes, JSON
from ..resources.MgmtObj import MgmtObj


defaultDeviceType = 'unknown'
defaultModel = "unknown"
defaultManufacturer = "unknown"
defaultDeviceLabel = "unknown serial id"

class DVI(MgmtObj):

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
			'dlb': None,
			'man': None,
			'mfdl': None,
			'mfd': None,
			'mod': None,
			'smod': None,
			'dty': None,
			'dvnm': None,
			'fwv': None,
			'swv': None,
			'hwv': None,
			'osv': None,
			'cnty': None,
			'loc': None,
			'syst': None,
			'spur': None,
			'purl': None,
			'ptl': None
	}


	def __init__(self, dct:Optional[JSON] = None, 
					   pi:Optional[str] = None, 
					   create:Optional[bool] = False) -> None:
		super().__init__(dct, pi, mgd = ResourceTypes.DVI, create = create)

		self.setAttribute('dty', defaultDeviceType, overwrite = False)
		self.setAttribute('mod', defaultModel, overwrite = False)
		self.setAttribute('man', defaultManufacturer, overwrite = False)
		self.setAttribute('dlb', defaultDeviceLabel, overwrite = False)


	#########################################################################
	#
	#	Resource specific
	#

	#	Database Related

	def getInsertQuery(self) -> Optional[str]:
		query = """
					INSERT INTO public.dvi(resource_index, mgd, obis, obps, dc, mgs, cmlk, dlb, man, mfdl, mfd, mod, smod, dty, dvnm, fwv, swv, hwv, osv, cnty, loc, syst, spur, purl, ptl)
					SELECT rt.index, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} FROM resource_table rt;
				"""

		return self._getInsertGeneralQuery() + query.format(
			self.validateAttributeValue(self['mgd']),
			self.validateAttributeValue(self['obis']),
			self.validateAttributeValue(self['obps']),
			self.validateAttributeValue(self['dc']),
			self.validateAttributeValue(self['mgs']),
			self.validateAttributeValue(self['cmlk']),
			self.validateAttributeValue(self['dlb']),
			self.validateAttributeValue(self['man']),
			self.validateAttributeValue(self['mfdl']),
			self.validateAttributeValue(self['mfd']),
			self.validateAttributeValue(self['mod']),
			self.validateAttributeValue(self['smod']),
			self.validateAttributeValue(self['dty']),
			self.validateAttributeValue(self['dvnm']),
			self.validateAttributeValue(self['fwv']),
			self.validateAttributeValue(self['swv']),
			self.validateAttributeValue(self['hwv']),
			self.validateAttributeValue(self['osv']),
			self.validateAttributeValue(self['cnty']),
			self.validateAttributeValue(self['loc']),
			self.validateAttributeValue(self['syst']),
			self.validateAttributeValue(self['spur']),
			self.validateAttributeValue(self['purl']),
			self.validateAttributeValue(self['ptl'])
		)