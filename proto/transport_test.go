package proto

import (
	"github.com/libopenstorage/gossip/api"
	"testing"
	"time"
)

type TestData struct {
	Data map[api.StoreKey]api.NodeInfo
}

func TestSendAndRcvData(t *testing.T) {
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

	ipString := "0.0.0.0:17003"
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
	r.Close()
	s.Close()

	if len(data1.Data) != len(data2.Data) {
		t.Error("Sent and rcvd messages mismatch, sent: ", data1,
			" got: ", data2)
	}
}
