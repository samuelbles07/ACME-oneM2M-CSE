#
#	testTS_TCI.py
#
#	(c) 2021 by Andreas Kraft
#	License: BSD 3-Clause License. See the LICENSE file for further details.
#
#	Unit tests for timeSeriean & timeSeries functionality
#

import unittest, sys
sys.path.append('../acme')
from typing import Tuple
from Constants import Constants as C
from Types import ResourceTypes as T, ResponseCode as RC
from init import *

maxBS	= 30
maxMdn	= 5
pei 	= int(timeSeriesInterval * 1000)
mdt 	= int(pei / 2)



class TestTS_TSI(unittest.TestCase):

	ae 			= None
	originator 	= None
	ts 			= None

	@classmethod
	@unittest.skipIf(noCSE, 'No CSEBase')
	def setUpClass(cls) -> None:
		dct = 	{ 'm2m:ae' : {
					'rn'  : aeRN, 
					'api' : 'NMyApp1Id',
				 	'rr'  : False,
				 	'srv' : [ '3' ]
				}}
		cls.ae, rsc = CREATE(cseURL, 'C', T.AE, dct)	# AE to work under
		assert rsc == RC.created, 'cannot create parent AE'
		cls.originator = findXPath(cls.ae, 'm2m:ae/aei')
		dct = 	{ 'm2m:ts' : { 
					'rn'  : tsRN,
					'mni' : 3
				}}
		cls.ts, rsc = CREATE(aeURL, cls.originator, T.TS, dct)
		assert rsc == RC.created, 'cannot create <timeSeries>'
		assert findXPath(cls.ts, 'm2m:ts/mni') == 3, 'mni is not correct'


	@classmethod
	@unittest.skipIf(noCSE, 'No CSEBase')
	def tearDownClass(cls) -> None:
		DELETE(aeURL, ORIGINATOR)	# Just delete the AE and everything below it. Ignore whether it exists or not

	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_addTSI(self) -> None:
		"""	Create <TSI> under <TS> """
		self.assertIsNotNone(TestTS_TSI.ae)
		self.assertIsNotNone(TestTS_TSI.ts)
		dct = 	{ 'm2m:tsi' : {
					'dgt' : (date := getDate()),
					'con' : 'aValue',
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)
		self.assertIsNotNone(r)
		self.assertIsNotNone(findXPath(r, 'm2m:tsi/ri'), r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'aValue', r)
		self.assertEqual(findXPath(r, 'm2m:tsi/dgt'), date, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/cs'), 6, r)
		self.rsiARi = findXPath(r, 'm2m:tsi/ri', r)			# store ri

		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cni'), r)
		self.assertIsInstance(findXPath(r, 'm2m:ts/cni'), int, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cni'), 1, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cbs'), r)
		self.assertIsInstance(findXPath(r, 'm2m:ts/cbs'), int, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cbs'), 6, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_addMoreTSI(self) -> None:
		"""	Create more <TSI>s under <TS> """
		dct = 	{ 'm2m:tsi' : {
					'dgt' : (date := getDate()),
					'con' : 'bValue'
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'bValue', r)
		self.assertEqual(findXPath(r, 'm2m:tsi/dgt'), date, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/cs'), 6, r)

		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cni'), r)
		self.assertIsInstance(findXPath(r, 'm2m:ts/cni'), int, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cni'), 2, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cbs'), 12, r)

		dct = 	{ 'm2m:tsi' : {
					'dgt' : (date := getDate()),
					'con' : 'cValue'
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'cValue', r)
		self.assertEqual(findXPath(r, 'm2m:tsi/dgt'), date, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/cs'), 6, r)

		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cni'), r)
		self.assertIsInstance(findXPath(r, 'm2m:ts/cni'), int, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cni'), 3)
		self.assertEqual(findXPath(r, 'm2m:ts/cbs'), 18, r)

		dct = 	{ 'm2m:tsi' : {
					'dgt' : (date := getDate()),
					'con' : 'dValue'
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'dValue', r)
		self.assertEqual(findXPath(r, 'm2m:tsi/dgt'), date, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/cs'), 6, r)

		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cni'), r)
		self.assertIsInstance(findXPath(r, 'm2m:ts/cni'), int, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cni'), 3, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cbs'), 18, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_retrieveTSLa(self) -> None:
		"""	Retrieve <TS>.LA """
		r, rsc = RETRIEVE(f'{tsURL}/la', TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(r, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/ty'), T.TSI, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'dValue', r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_retrieveTSOl(self) -> None:
		""" Retrieve <TS>.OL """
		r, rsc = RETRIEVE(f'{tsURL}/ol', TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(r, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/ty'), T.TSI, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'bValue', r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_changeTSMni(self) -> None:
		"""	Change <TS>.MNI to 1 -> OL == LA """
		dct = 	{ 'm2m:ts' : {
					'mni' : 1
 				}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dct)
		self.assertEqual(rsc, RC.updated, r)
		self.assertIsNotNone(r, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mni'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mni'), 1, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cni'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/cni'), 1, r)
		self.assertEqual(findXPath(r, 'm2m:ts/cbs'), 6, r)

		r, rsc = RETRIEVE(f'{tsURL}/la', TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(r, r)
		self.assertIsNotNone(findXPath(r, 'm2m:tsi/con'), r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'dValue', r)

		r, rsc = RETRIEVE(f'{tsURL}/ol', TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(r, r)
		self.assertIsNotNone(findXPath(r, 'm2m:tsi/con'), r)
		self.assertEqual(findXPath(r, 'm2m:tsi/con'), 'dValue', r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_deleteTS(self) -> None:
		"""	Delete <TS> """
		r, rsc = DELETE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.deleted, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSwithMBS(self) -> None:
		"""	Create <TS> with mbs"""
		dct = 	{ 'm2m:ts' : { 
					'rn'  : tsRN,
					'mbs' : maxBS
				}}
		TestTS_TSI.ts, rsc = CREATE(aeURL, TestTS_TSI.originator, T.TS, dct)
		self.assertEqual(rsc, RC.created, TestTS_TSI.ts)
		self.assertIsNotNone(findXPath(TestTS_TSI.ts, 'm2m:ts/mbs'), TestTS_TSI.ts)
		self.assertEqual(findXPath(TestTS_TSI.ts, 'm2m:ts/mbs'), maxBS, TestTS_TSI.ts)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIexactSize(self) -> None:
		"""	Add <TSI> to <TS> with exact max size"""
		dct = 	{ 'm2m:tsi' : {
					'dgt' : getDate(),
					'con' : 'x' * maxBS
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)
		self.assertEqual(findXPath(r, 'm2m:tsi/cs'), maxBS, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSItooLarge(self) -> None:
		"""	Add <TSI> to <TS> with size > mbs -> Fail """
		dct = 	{ 'm2m:tsi' : {
					'dgt' : getDate(),
					'con' : 'x' * (maxBS + 1)
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.notAcceptable, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIsForTSwithSize(self) -> None:
		"""	Add multiple <TSI>s to <TS> with size restrictions """
		# First fill up the container
		for _ in range(int(maxBS / 3)):
			dct = 	{ 'm2m:tsi' : {
						'dgt' : getDate(),
						'con' : 'x' * int(maxBS / 3)
					}}
			r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
			self.assertEqual(rsc, RC.created, r)
		
		# Test latest TSI for x
		r, rsc = RETRIEVE(f'{tsURL}/la', TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:tsi/con'), r)
		self.assertTrue(findXPath(r, 'm2m:tsi/con').startswith('x'), r)
		self.assertEqual(len(findXPath(r, 'm2m:tsi/con')), int(maxBS / 3), r)

		# Add another TSI
		dct = 	{ 'm2m:tsi' : {
					'dgt' : getDate(),
					'con' : 'y' * int(maxBS / 3)
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)

		# Test latest TSI for y
		r, rsc = RETRIEVE(f'{tsURL}/la', TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:tsi/con'), r)
		self.assertTrue(findXPath(r, 'm2m:tsi/con').startswith('y'), r)
		self.assertEqual(len(findXPath(r, 'm2m:tsi/con')), int(maxBS / 3), r)

		# Test TS
		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cni'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/cni'), 3, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/cbs'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/cbs'), maxBS, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIwithoutDGT(self) -> None:
		"""	Add <TSI> without DGT attribute -> Fail """
		dct = 	{ 'm2m:tsi' : {
					'con' : 'wrong'
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.badRequest, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIwithSameDGT(self) -> None:
		"""	Add <TSI>s with same DGT attribute -> Fail """
		dct = 	{ 'm2m:tsi' : {
					'dgt' : (date := getDate()),
					'con' : 'first'
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)
		dct = 	{ 'm2m:tsi' : {
					'dgt' : date,	# same date
					'con' : 'second'
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.conflict, r)	# CONFLICTs


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIwithSNR(self) -> None:
		"""	Add <TSI> with SNR"""
		dct = 	{ 'm2m:tsi' : {
					'dgt' : (date := getDate()),
					'con' : 'aValue',
					'snr' : 1
				}}
		r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
		self.assertEqual(rsc, RC.created, r)

		r, rsc = RETRIEVE(f'{tsURL}/la', TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:tsi/snr'), r)
		self.assertEqual(findXPath(r, 'm2m:tsi/snr'), 1, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSwithMonitoring(self) -> None:
		"""	Create <TS> with monitoring enabled"""
		dct = 	{ 'm2m:ts' : { 
					'rn'  : tsRN,
					'pei' : pei,
					'mdd' : True,
					'mdn' : maxMdn,
					'mdt' : mdt

				}}
		TestTS_TSI.ts, rsc = CREATE(aeURL, TestTS_TSI.originator, T.TS, dct)
		self.assertEqual(rsc, RC.created, TestTS_TSI.ts)
		self.assertIsNotNone(findXPath(TestTS_TSI.ts, 'm2m:ts/pei'), TestTS_TSI.ts)
		self.assertEqual(findXPath(TestTS_TSI.ts, 'm2m:ts/pei'), pei, TestTS_TSI.ts)
		self.assertIsNotNone(findXPath(TestTS_TSI.ts, 'm2m:ts/mdd'), TestTS_TSI.ts)
		self.assertTrue(findXPath(TestTS_TSI.ts, 'm2m:ts/mdd'), TestTS_TSI.ts)
		self.assertIsNone(findXPath(TestTS_TSI.ts, 'm2m:ts/mdlt'), TestTS_TSI.ts)	# empty mdlt is not created by default
		self.assertIsNotNone(findXPath(TestTS_TSI.ts, 'm2m:ts/mdc'), TestTS_TSI.ts)
		self.assertEqual(findXPath(TestTS_TSI.ts, 'm2m:ts/mdc'), 0, TestTS_TSI.ts)
		self.assertIsNotNone(findXPath(TestTS_TSI.ts, 'm2m:ts/mdt'), TestTS_TSI.ts)
		self.assertEqual(findXPath(TestTS_TSI.ts, 'm2m:ts/mdt'), mdt, TestTS_TSI.ts)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIinPeriod(self) -> None:
		"""	Add 3 <TSI> within the time period"""
		dctTs = 	{ 'm2m:ts' : { 
			'mdn' : maxMdn
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dctTs)
		self.assertEqual(rsc, RC.updated, r)

		date = datetime.datetime.utcnow().timestamp()
		for i in range(3):
			dct = 	{ 'm2m:tsi' : {
						'dgt' : toISO8601Date(date),
						'con' : 'aValue',
						'snr' : i
					}}
			r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
			self.assertEqual(rsc, RC.created, r)
			time.sleep(timeSeriesInterval) # == pei
			date += timeSeriesInterval

		# Check TS for missing TSI
		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdc'), 0, r)	# MissingDataCount == 0

		self._stopMonitoring()


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIinPeriodDgtTooSmall(self) -> None:
		"""	Add 1+3 <TSI> within the time period, but dgt is too small -> Fail"""
		dctTs = 	{ 'm2m:ts' : { 
			'mdn' : maxMdn
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dctTs)
		self.assertEqual(rsc, RC.updated, r)

		date = datetime.datetime.utcnow().timestamp()
		for i in range(4):
			dct = 	{ 'm2m:tsi' : {
						'dgt' : toISO8601Date(date),
						'con' : 'aValue',
						'snr' : i
					}}
			r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
			self.assertEqual(rsc, RC.created, r)
			time.sleep(timeSeriesInterval) # == pei
			date = datetime.datetime.utcnow().timestamp() + timeSeriesInterval - (timeSeriesInterval/2) - 0.1

		# Check TS for missing TSI
		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdc'), 3, r)	# MissingDataCount == 3

		self._stopMonitoring()


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSIinPeriodDgtTooLarge(self) -> None:
		"""	Add 1+3 <TSI> within the time period, but dgt is too large -> Fail"""
		dctTs = 	{ 'm2m:ts' : { 
			'mdn' : maxMdn
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dctTs)
		self.assertEqual(rsc, RC.updated, r)

		date = datetime.datetime.utcnow().timestamp()
		for i in range(4):
			dct = 	{ 'm2m:tsi' : {
						'dgt' : toISO8601Date(date),
						'con' : 'aValue',
						'snr' : i
					}}
			r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, dct)
			self.assertEqual(rsc, RC.created, r)
			date = datetime.datetime.utcnow().timestamp() + timeSeriesInterval + (timeSeriesInterval/2) + 0.1
			time.sleep(timeSeriesInterval) # == pei

		# Check TS for missing TSI
		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdc'), 3, r)	# MissingDataCount == 3

		self._stopMonitoring()


	def _stopMonitoring(self) -> None:
		""" Stop monitoring by removing the list. """
		dct = 	{ 'm2m:ts' : { 
			'mdn' : None
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dct)
		self.assertEqual(rsc, RC.updated, r)
		self.assertIsNone(findXPath(r, 'm2m:ts/mdn'), r)
		self.assertIsNone(findXPath(r, 'm2m:ts/mdlt'), r)
		self.assertIsNone(findXPath(r, 'm2m:ts/mdc'), r)


	def _createTSInotInPeriod(self, expectedMdc:int) -> None:
		"""	Add n <TSI> not within the time period """
		dct = 	{ 'm2m:ts' : { 
			'mdn' : maxMdn
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dct)
		self.assertEqual(rsc, RC.updated, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdt'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdt'), mdt, r)
		self.assertIsNone(findXPath(r, 'm2m:ts/mdlt'), r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdc'), 0, r)

		_pei = findXPath(r, 'm2m:ts/pei') / 1000.0
		_mdt = findXPath(r, 'm2m:ts/mdt') / 1000.0 
		for i in range(expectedMdc):
			tsidct = { 'm2m:tsi' : {
						'dgt' : (date := getDate()),
						'con' : 'aValue',
						'snr' : i
					}}
			r, rsc = CREATE(tsURL, TestTS_TSI.originator, T.TSI, tsidct)
			self.assertEqual(rsc, RC.created, r)
			time.sleep(_pei + (_mdt * 2.0))

			# r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
			# self.assertIsNotNone(findXPath(r, 'm2m:ts/mdlt'), r)
			# self.assertLessEqual(len(findXPath(r, 'm2m:ts/mdlt')), maxMdn, r)

		
		# Check TS for missing TSI
		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		if expectedMdc > maxMdn:
			self.assertGreaterEqual(findXPath(r, 'm2m:ts/mdc'), maxMdn, r)			# MissingDataCount == maxMdn
		else:
			self.assertGreaterEqual(findXPath(r, 'm2m:ts/mdc'), expectedMdc, r)	# MissingDataCount == expectedMdc


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSInotInPeriod(self) -> None:
		"""	Add <TSI> not within the time period """
		self._createTSInotInPeriod(3)

	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_createTSInotInPeriodLarger(self) -> None:
		"""	Add more <TSI> not within the time period """
		self._createTSInotInPeriod(maxMdn + 1)	# one more to check list size, 
		# dont remove list for next tests


	@unittest.skipIf(noCSE, 'No CSEBase')
	@unittest.skipIf(maxMdn < 3, 'mdn is set to < 3')
	def test_updateTSshortenMdlt(self) -> None:
		"""	Update <TS> MDN and shorten MDLT """
		dct = 	{ 'm2m:ts' : { 
			'mdn' : maxMdn - 2
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dct)
		self.assertEqual(rsc, RC.updated, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdn'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdn'), maxMdn - 2, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdlt'), r)
		self.assertEqual(len(findXPath(r, 'm2m:ts/mdlt')), maxMdn - 2, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdc'), maxMdn - 2, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_updateTSremoveMdn(self) -> None:
		"""	Update <TS> MDN with null and disable monitoring """
		self._stopMonitoring()


	@unittest.skipIf(noCSE, 'No CSEBase')
	@unittest.skipIf(maxMdn < 3, 'mdn is set to < 3')
	def test_updateTSaddMdn(self) -> None:
		"""	Update <TS> set MDN again and enable monitoring """
		# Set the detectTime to a short time
		dct = 	{ 'm2m:ts' : { 
			'mdn' : maxMdn - 2
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dct)
		self.assertEqual(rsc, RC.updated, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdn'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdn'), maxMdn - 2, r)
		self.assertIsNone(findXPath(r, 'm2m:ts/mdlt'), r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdc'), 0, r)


	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_updateTSaddMdt(self) -> None:
		"""	Update <TS> MDT with non-null and enable monitoring """
		# Set the detectTime to a short time
		dct = 	{ 'm2m:ts' : { 
			'mdt' : mdt
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dct)
		self.assertEqual(rsc, RC.updated, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdt'), r)
		self.assertEqual(findXPath(r, 'm2m:ts/mdt'), mdt, r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdn'), r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdlt'), r)
		self.assertIsNotNone(findXPath(r, 'm2m:ts/mdc'), r)
		

	@unittest.skipIf(noCSE, 'No CSEBase')
	def test_updateTSremoveMdt(self) -> None:
		"""	Update <TS> MDt with null and disable monitoring """
		# Set the detectTime to a short time
		dct = 	{ 'm2m:ts' : { 
			'mdt' : None
		}}
		r, rsc = UPDATE(tsURL, TestTS_TSI.originator, dct)
		self.assertEqual(rsc, RC.updated, r)
		self.assertIsNone(findXPath(r, 'm2m:ts/mdt'), r)

		# Wait a moment, retrieve and compare mdlt
		mdlt = findXPath(r, 'm2m:ts/mdlt')
		time.sleep(timeSeriesInterval)
		r, rsc = RETRIEVE(tsURL, TestTS_TSI.originator)
		self.assertEqual(rsc, RC.OK, r)
		self.assertEqual(mdlt, findXPath(r, 'm2m:ts/mdlt'), r)


# TODO: instead of mdt:9999 set the mdn to None etc.

# peid: Test with everything ok, but dgt < peid, and another test with dgt > peid
# TODO Test: first TSI with dgt < now - peid


def run(testVerbosity:int, testFailFast:bool) -> Tuple[int, int, int]:
	suite = unittest.TestSuite()
	suite.addTest(TestTS_TSI('test_addTSI'))
	suite.addTest(TestTS_TSI('test_addMoreTSI'))
	suite.addTest(TestTS_TSI('test_retrieveTSLa'))
	suite.addTest(TestTS_TSI('test_retrieveTSOl'))
	suite.addTest(TestTS_TSI('test_changeTSMni'))
	suite.addTest(TestTS_TSI('test_deleteTS'))
	
	suite.addTest(TestTS_TSI('test_createTSwithMBS'))
	suite.addTest(TestTS_TSI('test_createTSIexactSize'))
	suite.addTest(TestTS_TSI('test_createTSItooLarge'))
	suite.addTest(TestTS_TSI('test_createTSIsForTSwithSize'))
	suite.addTest(TestTS_TSI('test_createTSIwithoutDGT'))
	suite.addTest(TestTS_TSI('test_createTSIwithSameDGT'))
	suite.addTest(TestTS_TSI('test_createTSIwithSNR'))
	suite.addTest(TestTS_TSI('test_deleteTS'))

	suite.addTest(TestTS_TSI('test_createTSwithMonitoring'))
	suite.addTest(TestTS_TSI('test_createTSIinPeriod'))					# Start monitoring
	suite.addTest(TestTS_TSI('test_createTSInotInPeriod'))				# Start monitoring
	suite.addTest(TestTS_TSI('test_updateTSremoveMdn'))					# effectively stop monitoring
	suite.addTest(TestTS_TSI('test_createTSIinPeriodDgtTooSmall'))		# dgt too small
	suite.addTest(TestTS_TSI('test_createTSIinPeriodDgtTooLarge'))		# dgt too large
	suite.addTest(TestTS_TSI('test_createTSInotInPeriodLarger'))		# run the test again to overflow mdlt

	suite.addTest(TestTS_TSI('test_updateTSshortenMdlt'))
	suite.addTest(TestTS_TSI('test_updateTSremoveMdn'))					# effectively stop monitoring
	suite.addTest(TestTS_TSI('test_updateTSaddMdn'))
	suite.addTest(TestTS_TSI('test_createTSInotInPeriod'))
	suite.addTest(TestTS_TSI('test_updateTSaddMdt'))
	suite.addTest(TestTS_TSI('test_updateTSremoveMdt'))

	result = unittest.TextTestRunner(verbosity=testVerbosity, failfast=testFailFast).run(suite)
	
	printResult(result)
	return result.testsRun, len(result.errors + result.failures), len(result.skipped)


if __name__ == '__main__':
	_, errors, _ = run(2, True)
	sys.exit(errors)
