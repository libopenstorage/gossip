package proto

import (
	"github.com/libopenstorage/gossip/types"
	"strconv"
	"testing"
	"time"
)

type TestData struct {
	Data map[types.StoreKey]types.NodeInfo
}

func TestTransportSendAndRcvData(t *testing.T) {
	printTestInfo()
	time.Sleep(10 * time.Second)
	data1 := &TestData{}
	data2 := &TestData{}

	data1.Data = make(map[types.StoreKey]types.NodeInfo)
	data2.Data = make(map[types.StoreKey]types.NodeInfo)

	var handler types.OnMessageRcv = func(c types.MessageChannel) {
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
	time.Sleep(1 * time.Second)

	keyList := []types.StoreKey{"key1", "key2"}
	for i, key := range keyList {
		var node types.NodeInfo
		node.Id = types.NodeId(i)
		node.Value = "some data"
		data1.Data[key] = node
	}

	s := NewMessageChannel(ipString)
	if s == nil {
		t.Fatal("Error creating send channel, failing test")
	}
	go s.SendData(&data1)
	time.Sleep(1 * time.Second)
	s.Close()
	r.Close()

	if len(data1.Data) != len(data2.Data) {
		t.Error("Sent and rcvd messages mismatch, sent: ", data1,
			" got: ", data2)
	}
}

func TestTransportFailures(t *testing.T) {
	printTestInfo()
	time.Sleep(10 * time.Second)
	data1 := &TestData{}
	data2 := &TestData{}

	data1.Data = make(map[types.StoreKey]types.NodeInfo)
	data2.Data = make(map[types.StoreKey]types.NodeInfo)

	var handler types.OnMessageRcv = func(c types.MessageChannel) {
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

	keyList := []types.StoreKey{"key1", "key2"}
	for i, key := range keyList {
		var node types.NodeInfo
		node.Id = types.NodeId(i)
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
	time.Sleep(1 * time.Second)
	r.Close()

	// try sending data to closed end
	s = NewMessageChannel(ipString)
	if s != nil {
		t.Error("Error, expected nil sender")
	}

	// try sending non-marshabable data
	go r.RunOnRcvData()
	time.Sleep(1 * time.Second)
	s = NewMessageChannel(ipString)
	if s == nil {
		t.Fatal("Error creating send channel, failing test")
	}
	nonMarshalData := make(map[types.StoreKey]map[types.StoreKey]types.NodeInfoMap)
	err := s.SendData(nonMarshalData)
	if err != nil {
		t.Error("Expected error sending non-marshalable data")
	}
	s.Close()
	r.Close()
}

func TestTransportTwoWayExchange(t *testing.T) {
	printTestInfo()
	time.Sleep(10 * time.Second)
	data1 := &TestData{}
	data2 := &TestData{}
	data3 := &TestData{}
	data4 := &TestData{}

	data1.Data = make(map[types.StoreKey]types.NodeInfo)
	data2.Data = make(map[types.StoreKey]types.NodeInfo)
	data3.Data = make(map[types.StoreKey]types.NodeInfo)
	data4.Data = make(map[types.StoreKey]types.NodeInfo)

	var handler types.OnMessageRcv = func(c types.MessageChannel) {
		err := c.RcvData(&data2)
		if err != nil {
			t.Error("Error receiving data2: ", err)
		} else {
			t.Log("Done receiving")
		}

		for key, nodeInfo := range data2.Data {
			intId, _ := strconv.Atoi(string(nodeInfo.Id))
			nodeInfo.Id = types.NodeId(strconv.Itoa(intId + 1))
			data2.Data[key] = nodeInfo
		}
		err = c.SendData(data2)
		if err != nil {
			t.Error("Error sending data2: ", err)
		} else {
			t.Log("Done Sending data2")
		}
		time.Sleep(20 * time.Millisecond)

		err = c.RcvData(&data4)
		if err != nil {
			t.Error("Error sending data4: ", err)
		} else {
			t.Log("Done receving data4")
		}
		time.Sleep(20 * time.Millisecond)
	}

	r := NewRunnableMessageChannel("", handler)
	go r.RunOnRcvData()
	time.Sleep(1 * time.Second)

	keyList := []types.StoreKey{"key1", "key2"}
	for i, key := range keyList {
		var node types.NodeInfo
		node.Id = types.NodeId(i)
		node.Value = "some data"
		data1.Data[key] = node
	}

	ipString := "0.0.0.0"
	s := NewMessageChannel(ipString)
	if s == nil {
		t.Fatal("Error creating send channel, failing test")
	}
	s.SendData(&data1)
	time.Sleep(20 * time.Millisecond)

	err := s.RcvData(&data3)
	if err != nil {
		t.Fatal("Error receving data3: ", err)
	}
	for key, nodeInfo := range data3.Data {
		intId, _ := strconv.Atoi(string(nodeInfo.Id))
		nodeInfo.Id = types.NodeId(strconv.Itoa(intId + 1))
		data3.Data[key] = nodeInfo
	}
	time.Sleep(20 * time.Millisecond)

	err = s.SendData(&data3)
	if err != nil {
		t.Fatal("Error sending data3: ", err)
	}
	time.Sleep(20 * time.Millisecond)

	if len(data1.Data) != len(data2.Data) ||
		len(data1.Data) != len(data3.Data) ||
		len(data1.Data) != len(data4.Data) {
		t.Error("Data sent and received not matching Data1:",
			data1.Data, "\nData2:", data2.Data,
			"\nData3:", data3.Data, "\nData4:", data4.Data)
	}

	for key, nodeInfo := range data1.Data {
		nodeInfo2 := data2.Data[key]
		nodeInfo3 := data3.Data[key]
		nodeInfo4 := data4.Data[key]

		intId, _ := strconv.Atoi(string(nodeInfo.Id))
		id1 := types.NodeId(strconv.Itoa(intId + 1))
		id2 := types.NodeId(strconv.Itoa(intId + 2))
		if nodeInfo2.Id != id1 ||
			nodeInfo3.Id != id2 ||
			nodeInfo4.Id != id2 {
			t.Error("Data mismatch, Data1: ",
				nodeInfo, "\nData2:", nodeInfo2,
				"\nData3:", nodeInfo3, "\nData4:", nodeInfo4)
		}

		if nodeInfo2.Value != nodeInfo.Value ||
			nodeInfo3.Value != nodeInfo.Value ||
			nodeInfo4.Value != nodeInfo.Value {
			t.Error("Data mismatch, Data1: ",
				nodeInfo, "\nData2:", nodeInfo2,
				"\nData3:", nodeInfo3, "\nData4:", nodeInfo4)
		}
	}

	s.Close()
	r.Close()
}
