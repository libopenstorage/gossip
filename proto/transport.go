package proto

import (
	"bytes"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"net"
	"syscall"
)

// sendData serializes the given object and sends
// it over the given connection. Returns nil if
// it was successful, error otherwise
func sendData(obj interface{}, conn net.Conn) error {
	err := error(nil)
	buf, err := json.Marshal(obj)
	if err != nil {
		log.Error("Failed to serialize message", err)
		return err
	}

	for len(buf) > 0 {
		l, err := conn.Write(buf)
		if err != nil && err != syscall.EINTR {
			log.Error("Write failed: ", err)
			return err
		}
		buf = buf[l:]
	}

	return nil
}

// rcvData receives bytes over the connection
// until it can marshal the object. msg is the
// pointer to the object which will receive the data.
// Returns nil if it was successful, error otherwise.
func rcvData(msg interface{}, conn net.Conn) error {

	msgBuffer := new(bytes.Buffer)

	for {
		// XXX FIXME: What if the other node sends crap ?
		// this may never exit in such case
		_, err := msgBuffer.ReadFrom(conn)
		if err != nil && err != syscall.EINTR {
			log.Error("Error reading data from peer:", err)
			return err
		}

		err = json.Unmarshal(msgBuffer.Bytes(), msg)
		if err != nil {
			log.Warn("Received bad packet:", err)
			return err
		} else {
			return nil
		}
	}

	return nil
}
