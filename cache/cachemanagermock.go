// Code generated by MockGen. DO NOT EDIT.
// Source: ./cachemanager.go

// Package cache is a generated GoMock package.
package cache

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockICacheManager is a mock of ICacheManager interface.
type MockICacheManager struct {
	ctrl     *gomock.Controller
	recorder *MockICacheManagerMockRecorder
}

// MockICacheManagerMockRecorder is the mock recorder for MockICacheManager.
type MockICacheManagerMockRecorder struct {
	mock *MockICacheManager
}

// NewMockICacheManager creates a new mock instance.
func NewMockICacheManager(ctrl *gomock.Controller) *MockICacheManager {
	mock := &MockICacheManager{ctrl: ctrl}
	mock.recorder = &MockICacheManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockICacheManager) EXPECT() *MockICacheManagerMockRecorder {
	return m.recorder
}

// AddToSet mocks base method.
func (m *MockICacheManager) AddToSet(key, value string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddToSet", key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddToSet indicates an expected call of AddToSet.
func (mr *MockICacheManagerMockRecorder) AddToSet(key, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddToSet", reflect.TypeOf((*MockICacheManager)(nil).AddToSet), key, value)
}

// Connect mocks base method.
func (m *MockICacheManager) Connect() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Connect")
	ret0, _ := ret[0].(error)
	return ret0
}

// Connect indicates an expected call of Connect.
func (mr *MockICacheManagerMockRecorder) Connect() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Connect", reflect.TypeOf((*MockICacheManager)(nil).Connect))
}

// CopySet mocks base method.
func (m *MockICacheManager) CopySet(fromKey, toKey string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CopySet", fromKey, toKey)
	ret0, _ := ret[0].(error)
	return ret0
}

// CopySet indicates an expected call of CopySet.
func (mr *MockICacheManagerMockRecorder) CopySet(fromKey, toKey interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CopySet", reflect.TypeOf((*MockICacheManager)(nil).CopySet), fromKey, toKey)
}

// DeleteFromSet mocks base method.
func (m *MockICacheManager) DeleteFromSet(key, value string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteFromSet", key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteFromSet indicates an expected call of DeleteFromSet.
func (mr *MockICacheManagerMockRecorder) DeleteFromSet(key, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteFromSet", reflect.TypeOf((*MockICacheManager)(nil).DeleteFromSet), key, value)
}

// DeleteSet mocks base method.
func (m *MockICacheManager) DeleteSet(key string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteSet", key)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteSet indicates an expected call of DeleteSet.
func (mr *MockICacheManagerMockRecorder) DeleteSet(key interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteSet", reflect.TypeOf((*MockICacheManager)(nil).DeleteSet), key)
}

// Disconnect mocks base method.
func (m *MockICacheManager) Disconnect() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Disconnect")
	ret0, _ := ret[0].(error)
	return ret0
}

// Disconnect indicates an expected call of Disconnect.
func (mr *MockICacheManagerMockRecorder) Disconnect() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Disconnect", reflect.TypeOf((*MockICacheManager)(nil).Disconnect))
}

// GetAllFromSet mocks base method.
func (m *MockICacheManager) GetAllFromSet(key string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllFromSet", key)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllFromSet indicates an expected call of GetAllFromSet.
func (mr *MockICacheManagerMockRecorder) GetAllFromSet(key interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllFromSet", reflect.TypeOf((*MockICacheManager)(nil).GetAllFromSet), key)
}

// GetFromSet mocks base method.
func (m *MockICacheManager) GetFromSet(key string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFromSet", key)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFromSet indicates an expected call of GetFromSet.
func (mr *MockICacheManagerMockRecorder) GetFromSet(key interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFromSet", reflect.TypeOf((*MockICacheManager)(nil).GetFromSet), key)
}

// GetLength mocks base method.
func (m *MockICacheManager) GetLength(key string) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLength", key)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetLength indicates an expected call of GetLength.
func (mr *MockICacheManagerMockRecorder) GetLength(key interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLength", reflect.TypeOf((*MockICacheManager)(nil).GetLength), key)
}

// MoveSet mocks base method.
func (m *MockICacheManager) MoveSet(fromKey, toKey string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MoveSet", fromKey, toKey)
	ret0, _ := ret[0].(error)
	return ret0
}

// MoveSet indicates an expected call of MoveSet.
func (mr *MockICacheManagerMockRecorder) MoveSet(fromKey, toKey interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MoveSet", reflect.TypeOf((*MockICacheManager)(nil).MoveSet), fromKey, toKey)
}

// PopFromSet mocks base method.
func (m *MockICacheManager) PopFromSet(key string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PopFromSet", key)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PopFromSet indicates an expected call of PopFromSet.
func (mr *MockICacheManagerMockRecorder) PopFromSet(key interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PopFromSet", reflect.TypeOf((*MockICacheManager)(nil).PopFromSet), key)
}
