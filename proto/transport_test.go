package proto

import (
	"github.com/libopenstorage/gossip/api"
	"testing"
	"time"
)

type TestData struct {
	Data map[api.StoreKey]api.NodeInfo
}

func TestTransportSendAndRcvData(t *testing.T) {
	data1 := &TestData{}
	data2 := &TestData{}

	data1.Data = make(map[api.StoreKey]api.NodeInfo)
	data2.Data = make(map[api.StoreKey]api.NodeInfo)

	var handler api.OnMessageRcv = func(c api.MessageChannel) {
		err := c.RcvData(&data2)
		if err != nil {
			t.Error("Error receiving data: ", err)
		} else {
			t.Log("Done receiving")
		}
	}

	ipString := "0.0.0.0:9002"
	r := NewRunnableMessageChannel(ipString, handler)
	go r.RunOnRcvData()
	time.Sleep(5 * time.Second)

	keyList := []api.StoreKey{"key1", "key2"}
	for i, key := range keyList {
		var node api.NodeInfo
		node.Id = api.NodeId(i)
		node.Value = "some data"
		data1.Data[key] = node
	}

	s := NewMessageChannel(ipString)
	if s == nil {
		t.Fatal("Error creating send channel, failing test")
	}
	go s.SendData(&data1)
	time.Sleep(5 * time.Second)
	s.Close()
	r.Close()

	if len(data1.Data) != len(data2.Data) {
		t.Error("Sent and rcvd messages mismatch, sent: ", data1,
			" got: ", data2)
	}
}

func TestTransportFailures(t *testing.T) {
	data1 := &TestData{}
	data2 := &TestData{}

	data1.Data = make(map[api.StoreKey]api.NodeInfo)
	data2.Data = make(map[api.StoreKey]api.NodeInfo)

	var handler api.OnMessageRcv = func(c api.MessageChannel) {
		err := c.RcvData(&data2)
		if err == nil {
			t.Error("Did not receive expected error")
		} else {
			t.Log("Error receiving data: ", err)
		}
		return
	}

	ipString := "0.0.0.0:17006"
	r := NewRunnableMessageChannel(ipString, handler)
	go r.RunOnRcvData()
	time.Sleep(5 * time.Second)

	keyList := []api.StoreKey{"key1", "key2"}
	for i, key := range keyList {
		var node api.NodeInfo
		node.Id = api.NodeId(i)
		node.Value = "some data"
		data1.Data[key] = node
	}

	// close the channel without sending any message
	s := NewMessageChannel(ipString)
	if s == nil {
		t.Fatal("Error creating send channel, failing test")
	}
	time.Sleep(10 * time.Millisecond)
	s.Close()
	time.Sleep(10 * time.Millisecond)
	r.Close()
	time.Sleep(10 * time.Millisecond)

	// open and then close the channel
	go r.RunOnRcvData()
	time.Sleep(5 * time.Second)
	r.Close()

	// try sending data to closed end
	s = NewMessageChannel(ipString)
	if s != nil {
		t.Error("Error, expected nil sender")
	}
}
