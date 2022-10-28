#
#	CNT_LA.py
#
#	(c) 2020 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	ResourceType: latest (virtual resource)
#

"""	This module implements the virtual <latest> resource type for <container> resources.
"""

from __future__ import annotations
from typing import Optional
from ..etc.Types import AttributePolicyDict, ResourceTypes as T, ResponseStatusCode as RC, Result, JSON, CSERequest
from ..services import CSE
from ..services.Logging import Logging as L
from ..resources.VirtualResource import VirtualResource


class CNT_LA(VirtualResource):
	"""	This class implements the virtual <latest> resource for <container> resources.
	"""

	_allowedChildResourceTypes:list[T] = [ ]
	"""	A list of allowed child-resource types for this resource type. """

	_attributes:AttributePolicyDict = {		
		# None for virtual resources
	}
	""" A dictionary of the attributes and attribute policies for this resource type. 
		The attribute policies are assigned during startup by the `Importer`.
	"""


	def __init__(self, dct:Optional[JSON] = None, 
					   pi:Optional[str] = None, 
					   create:Optional[bool] = False) -> None:
		super().__init__(T.CNT_LA, dct, pi, create = create, inheritACP = True, readOnly = True, rn = 'la')


	def handleRetrieveRequest(self, request:Optional[CSERequest] = None,
									id:Optional[str] = None,
									originator:str = None) -> Result:
		""" Handle a RETRIEVE request.

			Args:
				request: The original request.
				id: Resource ID of the original request.
				originator: The request's originator.

			Return:
				The latest <contentInstance> for the parent <container>, or an error `Result`.
		"""
		L.isDebug and L.logDebug('Retrieving latest CIN from CNT')
		return self.retrieveLatestOldest(request, originator, T.CIN, oldest = False)


	def handleCreateRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		""" Handle a CREATE request. 

			Args:
				request: The request to process.
				id: The structured or unstructured resource ID of the target resource.
				originator: The request's originator.
			
			Return:
				Fails with error code for this resource type. 
		"""
		return Result.errorResult(rsc = RC.operationNotAllowed, dbg = 'CREATE operation not allowed for <latest> resource type')


	def handleUpdateRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		""" Handle an UPDATE request.			
	
			Args:
				request: The request to process.
				id: The structured or unstructured resource ID of the target resource.
				originator: The request's originator.
			
			Return:
				Fails with error code for this resource type. 
		"""
		return Result.errorResult(rsc = RC.operationNotAllowed, dbg = 'UPDATE operation not allowed for <latest> resource type')


	def handleDeleteRequest(self, request:CSERequest, id:str, originator:str) -> Result:
		""" Handle a DELETE request.

			Delete the latest resource.

			Args:
				request: The request to process.
				id: The structured or unstructured resource ID of the target resource.
				originator: The request's originator.
			
			Return:
				Result object indicating success or failure.
		"""
		L.isDebug and L.logDebug('Deleting latest CIN from CNT')
		if not (r := CSE.dispatcher.retrieveLatestOldestInstance(self.pi, T.CIN)):
			return Result.errorResult(rsc = RC.notFound, dbg='no instance for <latest>')
		return CSE.dispatcher.deleteLocalResource(r, originator, withDeregistration = True)
