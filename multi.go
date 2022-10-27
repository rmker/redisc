package redisc

import (
	"errors"
	"fmt"
)

// Support MSET/MGET command based on the PipeLiner, which is a redis.Conn interface for redis pipeline.
// multixxx function receives keys from args, calculates all slots of keys, and then generates multiple new MSET/MGET commands based on slots.
// All these commands will be sent to the corresponding nodes by PipeLiner. The PipeLiner would handle that automatically if redirect error occurs.
// Eventually, all responses from different nodes will be rebuilt into a single response based on the command key order.

// multiset supports MSET command for a redis cluster based on pipeLiner
func multiset(c *Conn, args ...interface{}) (interface{}, error) {
	var (
		orderSlots []int
		res        interface{}
	)
	if len(args) <= 0 {
		return nil, nil
	}
	cmdMap := make(map[int][]interface{})
	for i := 0; i < len(args); i = i + 2 {
		vk := i + 1
		if vk >= len(args) {
			// args length exeption for mset
			return nil, errors.New("args length exeption for mset")
		}
		key := fmt.Sprintf("%s", args[i])
		slot := Slot(key)
		_, exist := cmdMap[slot]
		if !exist {
			cmdMap[slot] = []interface{}{key, args[vk]}
		} else {
			cmdMap[slot] = append(cmdMap[slot], key, args[vk])
		}
	}

	pipeLiner := c.cluster.GetPipelinerConn()
	defer pipeLiner.Close()
	for slot := range cmdMap {
		err := pipeLiner.Send(CMDMSET, cmdMap[slot]...)
		if err != nil {
			return nil, err
		}
		orderSlots = append(orderSlots, slot)
	}
	err := pipeLiner.Flush()
	if err != nil {
		return nil, err
	}
	for range orderSlots {
		reply, err := pipeLiner.Receive()
		if err != nil {
			return nil, err
		}
		res = reply
	}
	return res, nil
}

// multiget supports MGET command for a redis cluster based on pipeLiner
func multiget(c *Conn, args ...interface{}) (interface{}, error) {
	var (
		orderSlots []int
		res        []interface{}
		keys       []string
	)
	if len(args) <= 0 {
		return nil, nil
	}
	cmdMap := make(map[int][]interface{})
	for _, arg := range args {
		key := fmt.Sprintf("%s", arg)
		keys = append(keys, key)
		slot := Slot(key)
		_, exist := cmdMap[slot]
		if !exist {
			cmdMap[slot] = []interface{}{key}
		} else {
			cmdMap[slot] = append(cmdMap[slot], key)
		}
	}
	pipeLiner := c.cluster.GetPipelinerConn()
	defer pipeLiner.Close()
	for slot := range cmdMap {
		err := pipeLiner.Send(CMDMGET, cmdMap[slot]...)
		if err != nil {
			return nil, err
		}
		orderSlots = append(orderSlots, slot)
	}
	err := pipeLiner.Flush()
	if err != nil {
		return nil, err
	}

	resMap := make(map[string]interface{})
	for _, slot := range orderSlots {
		reply, err := pipeLiner.Receive()
		if err != nil {
			return nil, err
		}
		keyCount := len(cmdMap[slot])
		replySlice, ok := reply.([]interface{})
		validReply := false
		if ok && keyCount == len(replySlice) {
			validReply = true
		}
		for i := 0; i < keyCount; i++ {
			key := fmt.Sprintf("%s", cmdMap[slot][i])
			if validReply {
				resMap[key] = replySlice[i]
			} else {
				resMap[key] = nil
			}
		}
	}
	for i := range keys {
		res = append(res, resMap[keys[i]])
	}
	return res, nil
}
