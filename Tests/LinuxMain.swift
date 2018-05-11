// Generated using Sourcery 0.13.0 — https://github.com/krzysztofzablocki/Sourcery
// DO NOT EDIT

import XCTest
@testable import danakeTests

extension BatchTests {
  static var allTests = [
    ("testInsertAsyncNoClosure", testInsertAsyncNoClosure),
    ("testInsertAsyncWithClosure", testInsertAsyncWithClosure),
    ("testCommit", testCommit),
    ("testCommitWithUnrecoverableError", testCommitWithUnrecoverableError),
    ("testCommitWithError", testCommitWithError),
    ("testCommitWithBatchTimeout", testCommitWithBatchTimeout),
    ("testDispatchTimeIntervalExtension", testDispatchTimeIntervalExtension),
    ("testNoCommitLogging", testNoCommitLogging),
    ("testCommitSync", testCommitSync),
    ("testRandomTimeout", testRandomTimeout),
  ]
}

extension DatabaseTests {
  static var allTests = [
    ("testDatabaseCreation", testDatabaseCreation),
    ("testRetrievalResult", testRetrievalResult),
    ("testRegistrar", testRegistrar),
    ("testValidationResult", testValidationResult),
    ("testEntityCreation", testEntityCreation),
  ]
}

extension DateExtensionTests {
  static var allTests = [
    ("testRoughlyEquals ", testRoughlyEquals ),
  ]
}

extension EntityCommitDirtyTests {
  static var allTests = [
    ("testCommitDirty", testCommitDirty),
    ("testCommitDirtyPendingUpdate", testCommitDirtyPendingUpdate),
    ("testCommitDirtyPendingUpdateWithErrors", testCommitDirtyPendingUpdateWithErrors),
    ("testCommitDirtyPendingUpdateWithTimeouts", testCommitDirtyPendingUpdateWithTimeouts),
    ("testCommitDirty2PendingUpdates", testCommitDirty2PendingUpdates),
    ("testCommitDirty2PendingUpdatesWithErrors", testCommitDirty2PendingUpdatesWithErrors),
    ("testCommitDirty2PendingUpdatesWithTimeouts", testCommitDirty2PendingUpdatesWithTimeouts),
    ("testCommitDirtyPendingRemoveUpdate", testCommitDirtyPendingRemoveUpdate),
    ("testCommitDirtyPendingRemoveUpdateWithErrors", testCommitDirtyPendingRemoveUpdateWithErrors),
    ("testCommitDirtyPendingRemoveUpdateWithTimeouts", testCommitDirtyPendingRemoveUpdateWithTimeouts),
    ("testCommitDirtyPendingRemove", testCommitDirtyPendingRemove),
    ("testCommitDirtyPendingRemoveWithErrors", testCommitDirtyPendingRemoveWithErrors),
    ("testCommitDirtyPendingRemoveWithTimeouts", testCommitDirtyPendingRemoveWithTimeouts),
    ("testCommitDirty2PendingRemoves", testCommitDirty2PendingRemoves),
    ("testCommitDirty2PendingRemovesWithErrors", testCommitDirty2PendingRemovesWithErrors),
    ("testCommitDirty2PendingRemovesWithTimeouts", testCommitDirty2PendingRemovesWithTimeouts),
    ("testCommitDirtyPendingUpdateRemove", testCommitDirtyPendingUpdateRemove),
    ("testCommitDirtyPendingUpdateRemoveWithErrors", testCommitDirtyPendingUpdateRemoveWithErrors),
    ("testCommitDirtyPendingUpdateRemoveWithTimeoutss", testCommitDirtyPendingUpdateRemoveWithTimeoutss),
  ]
}

extension EntityCommitNewTests {
  static var allTests = [
    ("testCommitNew", testCommitNew),
    ("testCommitNewPendingUpdate", testCommitNewPendingUpdate),
    ("testCommitNewPendingUpdateWithErrors", testCommitNewPendingUpdateWithErrors),
    ("testCommitNewPendingUpdateWithTimeouts", testCommitNewPendingUpdateWithTimeouts),
    ("testCommitNew2PendingUpdates", testCommitNew2PendingUpdates),
    ("testCommitNew2PendingUpdatesWithErrors", testCommitNew2PendingUpdatesWithErrors),
    ("testCommitNew2PendingUpdatesWithTimeouts", testCommitNew2PendingUpdatesWithTimeouts),
    ("testCommitNewPendingRemoveUpdate", testCommitNewPendingRemoveUpdate),
    ("testCommitNewPendingRemoveUpdateWithErrors", testCommitNewPendingRemoveUpdateWithErrors),
    ("testCommitNewPendingRemoveUpdateWithTimeouts", testCommitNewPendingRemoveUpdateWithTimeouts),
    ("testCommitNewPendingRemove", testCommitNewPendingRemove),
    ("testCommitNewPendingRemoveWithErrors", testCommitNewPendingRemoveWithErrors),
    ("testCommitNewPendingRemoveWithTimeout", testCommitNewPendingRemoveWithTimeout),
    ("testCommitNew2PendingRemoves", testCommitNew2PendingRemoves),
    ("testCommitNew2PendingRemovesWithErrors", testCommitNew2PendingRemovesWithErrors),
    ("testCommitNew2PendingRemovesWithTimeouts", testCommitNew2PendingRemovesWithTimeouts),
    ("testCommitNewPendingUpdateRemove", testCommitNewPendingUpdateRemove),
    ("testCommitNewPendingUpdateRemoveWithErrors", testCommitNewPendingUpdateRemoveWithErrors),
    ("testCommitNewPendingUpdateRemoveWithTimeouts", testCommitNewPendingUpdateRemoveWithTimeouts),
  ]
}

extension EntityCommitPendingRemovalTests {
  static var allTests = [
    ("testCommitPendingRemove", testCommitPendingRemove),
    ("testCommitPendingRemovePendingUpdate", testCommitPendingRemovePendingUpdate),
    ("testCommitPendingRemovePendingUpdateWithErrors", testCommitPendingRemovePendingUpdateWithErrors),
    ("testCommitPendingRemovePendingUpdateWithTimeouts", testCommitPendingRemovePendingUpdateWithTimeouts),
    ("testCommitPendingRemove2PendingUpdates", testCommitPendingRemove2PendingUpdates),
    ("testCommitPendingRemove2PendingUpdatesWithErrors", testCommitPendingRemove2PendingUpdatesWithErrors),
    ("testCommitPendingRemove2PendingUpdatesWithTimeouts", testCommitPendingRemove2PendingUpdatesWithTimeouts),
    ("testCommitPendingRemovePendingRemoveUpdate", testCommitPendingRemovePendingRemoveUpdate),
    ("testCommitPendingRemovePendingRemoveUpdateWithErrors", testCommitPendingRemovePendingRemoveUpdateWithErrors),
    ("testCommitPendingRemovePendingRemoveUpdateWithTimeouts", testCommitPendingRemovePendingRemoveUpdateWithTimeouts),
    ("testCommitPendingRemovePendingRemove", testCommitPendingRemovePendingRemove),
    ("testCommitPendingRemovePendingRemoveWithErrors", testCommitPendingRemovePendingRemoveWithErrors),
    ("testCommitPendingRemovePendingRemoveWithTimeouts", testCommitPendingRemovePendingRemoveWithTimeouts),
    ("testCommitPendingRemove2PendingRemoves", testCommitPendingRemove2PendingRemoves),
    ("testCommitPendingRemove2PendingRemovesWithErrors", testCommitPendingRemove2PendingRemovesWithErrors),
    ("testCommitPendingRemove2PendingRemovesWithTimeouts", testCommitPendingRemove2PendingRemovesWithTimeouts),
    ("testCommitPendingRemovePendingUpdateRemove", testCommitPendingRemovePendingUpdateRemove),
    ("testCommitPendingRemovePendingUpdateRemoveWithErrors", testCommitPendingRemovePendingUpdateRemoveWithErrors),
    ("testCommitPendingRemovePendingUpdateRemoveWithTimeouts", testCommitPendingRemovePendingUpdateRemoveWithTimeouts),
  ]
}

extension EntityCommitTests {
  static var allTests = [
    ("testEntityCommitPersistent", testEntityCommitPersistent),
    ("testEntityCommitAbandoned", testEntityCommitAbandoned),
    ("testEntityCommitSaving", testEntityCommitSaving),
  ]
}

extension EntityReferenceTests {
  static var allTests = [
    ("testCreationEncodeDecode", testCreationEncodeDecode),
    ("testGetReference", testGetReference),
    ("testWillUpdate", testWillUpdate),
    ("testAddParentToBatch", testAddParentToBatch),
    ("testSetEntity", testSetEntity),
    ("testSetReferenceData", testSetReferenceData),
    ("testSetReferenceDataIsEager", testSetReferenceDataIsEager),
    ("testAsync", testAsync),
    ("testGet", testGet),
    ("testSetWithinEntity", testSetWithinEntity),
  ]
}

extension EntityTests {
  static var allTests = [
    ("testCreation", testCreation),
    ("testSettersGetters", testSettersGetters),
    ("testReadAccess", testReadAccess),
    ("testWriteAccess", testWriteAccess),
    ("testSetDirty", testSetDirty),
    ("testEncodeDecode", testEncodeDecode),
    ("testDecodeEntityReference", testDecodeEntityReference),
    ("testAnyEntity", testAnyEntity),
    ("testEntityPersistenceWrapper", testEntityPersistenceWrapper),
    ("testHandleActionUpdateItem ", testHandleActionUpdateItem ),
    ("testHandleActionSetDirty ", testHandleActionSetDirty ),
    ("testHandleActionRemove ", testHandleActionRemove ),
    ("testRemove ", testRemove ),
    ("testAsData", testAsData),
    ("testPersistenceStatePair", testPersistenceStatePair),
    ("testEntityReferenceData", testEntityReferenceData),
    ("testEntityReferenceSerializationData", testEntityReferenceSerializationData),
  ]
}

extension InMemoryAccessorTests {
  static var allTests = [
    ("testInMemoryAccessor", testInMemoryAccessor),
    ("testDecoder", testDecoder),
    ("testEncoder", testEncoder),
    ("testCount", testCount),
    ("testSetThrowError", testSetThrowError),
    ("testGetWithDeserializationClosure", testGetWithDeserializationClosure),
    ("testScanWithDeserializationClosure", testScanWithDeserializationClosure),
  ]
}

extension LoggingTests {
  static var allTests = [
    ("testLogLevel", testLogLevel),
    ("testFormattedData", testFormattedData),
    ("testStandardFormat", testStandardFormat),
    ("testLogEntry", testLogEntry),
    ("testInMemoryLogger", testInMemoryLogger),
  ]
}

extension ParallelTests {
  static var allTests = [
    ("testParallel", testParallel),
  ]
}

extension PersistentCollectionTests {
  static var allTests = [
    ("testCreation", testCreation),
    ("testCreationInvalidName", testCreationInvalidName),
    ("testPersistenceCollectionNew", testPersistenceCollectionNew),
    ("testGet", testGet),
    ("testPerssistentCollectionGetParallel", testPerssistentCollectionGetParallel),
    ("testGetAsync ", testGetAsync ),
    ("testScan", testScan),
    ("testScanAsync", testScanAsync),
  ]
}

extension SampleTests {
  static var allTests = [
    ("testInMemorySample", testInMemorySample),
    ("testDemonstrateThrowError", testDemonstrateThrowError),
    ("testDemonstratePrefetchWithGet ", testDemonstratePrefetchWithGet ),
    ("testDemonstrateUpdateErrors ", testDemonstrateUpdateErrors ),
    ("testCompanyCreation", testCompanyCreation),
    ("testCompanyEncodeDecode", testCompanyEncodeDecode),
    ("testCompanyEmployees", testCompanyEmployees),
    ("testCompanyEmployeesAsync", testCompanyEmployeesAsync),
    ("testCompanyCollectionNew", testCompanyCollectionNew),
    ("testAddressCreation", testAddressCreation),
    ("testAddressFromCollection", testAddressFromCollection),
    ("testEmployeeCreation", testEmployeeCreation),
    ("testEmployeeFromCollection", testEmployeeFromCollection),
    ("testInMemoryAccessorEmployeesForCompany", testInMemoryAccessorEmployeesForCompany),
    ("testEmployeeCollectionForCompany", testEmployeeCollectionForCompany),
    ("testEmployeeCollectionForCompanyAsync", testEmployeeCollectionForCompanyAsync),
    ("testRemoveAll", testRemoveAll),
  ]
}


XCTMain([
  testCase(BatchTests.allTests),
  testCase(DatabaseTests.allTests),
  testCase(DateExtensionTests.allTests),
  testCase(EntityCommitDirtyTests.allTests),
  testCase(EntityCommitNewTests.allTests),
  testCase(EntityCommitPendingRemovalTests.allTests),
  testCase(EntityCommitTests.allTests),
  testCase(EntityReferenceTests.allTests),
  testCase(EntityTests.allTests),
  testCase(InMemoryAccessorTests.allTests),
  testCase(LoggingTests.allTests),
  testCase(ParallelTests.allTests),
  testCase(PersistentCollectionTests.allTests),
  testCase(SampleTests.allTests),
])
