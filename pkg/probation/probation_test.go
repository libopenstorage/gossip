package probation

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testProbationTimeout = 5 * time.Second
	testWaitTime         = 8 * time.Second
	testStartWaitTime    = 3 * time.Second
)

func TestProbationAdd(t *testing.T) {
	p := setup()
	err := p.Start()
	time.Sleep(testStartWaitTime)

	require.NoError(t, err, "Failed to Start")
	err = p.Add("client1", nil, true)
	require.NoError(t, err, "Failed to Add")
}

func TestProbationAddTo2ndQueue(t *testing.T) {
	p := setup()
	err := p.Start()
	time.Sleep(testWaitTime)

	require.NoError(t, err, "Failed to Start")
	err = p.Add("client1", nil, true)
	require.NoError(t, err, "Failed to Add")
}

func TestProbationExpiry(t *testing.T) {
	p := setup()
	err := p.Start()
	time.Sleep(testStartWaitTime)

	require.NoError(t, err, "Failed to Start")
	_, err = os.Create(testFileName("client1"))
	require.NoError(t, err, "Expected no error on Create")
	err = p.Add("client1", nil, true)
	require.NoError(t, err, "Failed to Add")

	time.Sleep(testWaitTime)

	_, err = os.Stat(testFileName("client1"))
	require.True(t, os.IsNotExist(err), "Expected callback fn to be executed")
}

func TestProbationRemove(t *testing.T) {
	p := setup()
	err := p.Start()
	time.Sleep(testStartWaitTime)

	require.NoError(t, err, "Failed to Start")
	err = p.Add("client3", nil, true)
	require.NoError(t, err, "Failed to Add")
	err = p.Add("client4", nil, true)
	require.NoError(t, err, "Failed to Add")

	_, err = os.Create(testFileName("client3"))
	require.NoError(t, err, "Expected no error on Create")
	_, err = os.Create(testFileName("client4"))
	require.NoError(t, err, "Expected no error on Create")

	time.Sleep(1 * time.Second)

	err = p.Remove("client3")

	time.Sleep(testWaitTime)

	_, err = os.Stat(testFileName("client3"))
	require.NoError(t, err, "Expected callback fn to be not executed for client3")

	_, err = os.Stat(testFileName("client4"))
	require.True(t, os.IsNotExist(err), "Expected callback fn to be executed for client4")
}

func testCallback(clientID string, clientData interface{}) error {
	return os.RemoveAll(testFileName(clientID))
}

func testFileName(clientID string) string {
	return "/tmp/tracker/" + clientID
}

func setup() Probation {
	os.RemoveAll("/tmp/tracker/")
	os.MkdirAll("/tmp/tracker/", os.ModeDir)
	return NewProbationManager("test", testProbationTimeout, testCallback)
}
