package redisc

import (
	"errors"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Support pipeline command and redirect handling for a redis cluster.
// You can use it to run a pipeline in a redis cluster(without proxy), just like using a redis.Conn.
// PipeLiner splits a pipeline into multiple batches according to the keys commands, then runs every batch in goroutines concurrently.
// Every batch is a real pipeline request to a redis node. Every response will be stored in the cmd.reply until all requests got their request.
// If MOVED response occurs, the pipeLiner will invoke Cluster.Refresh to refresh the cluster slots mapping, and then handle them in the new
// batches according the addresses for new nodes.

type cmd struct {
	commandName string
	args        []interface{}
	reply       interface{}
	reply_err   error
	slot        int
	addr        string
	re          *RedirError
}

// batch includes the commands corresponding a same redis node. A real redis pipeline will be run when a batch runs
type batch struct {
	addr string
	conn redis.Conn
	cmds []*cmd
}

// pipeLiner is a struct that implements redis.Conn interface. It is used to handle pipeline command in a redis cluster
type pipeLiner struct {
	c         *Cluster
	connForDo redis.Conn
	cmds      []*cmd
	forceDial bool
	readOnly  bool
	flushed   bool
	recvPos   int
	batches   map[string]*batch
}

func newPipeliner(c *Cluster) redis.Conn {
	return &pipeLiner{
		c:       c,
		recvPos: -1,
	}
}

// Run a batch that do the real redis pipeline request
func (bt *batch) run(p *pipeLiner) error {
	var err error
	if bt.conn == nil && len(bt.addr) > 0 {
		bt.conn = p.c.GetConnForAddr(bt.addr, p.forceDial, p.readOnly)
	}
	if bt.conn == nil {
		return errors.New("GetConnForAddr failed")
	}
	for _, cmd := range bt.cmds {
		err = bt.conn.Send(cmd.commandName, cmd.args...)
		if err != nil {
			return err
		}
	}
	err = bt.conn.Flush()
	if err != nil {
		return err
	}
	for _, cmd := range bt.cmds {
		cmd.reply, cmd.reply_err = bt.conn.Receive()
		if cmd.reply_err != nil {
			if re := ParseRedir(cmd.reply_err); re != nil {
				cmd.re = re
				if re.Type == "MOVED" {
					p.c.needsRefresh(re)
				}
			}
		}
	}
	return nil
}

// Build the batches into the batches map
func (p *pipeLiner) buildBatches() error {
	slots := make([]int, len(p.cmds))
	for i, cmd := range p.cmds {
		if cmd != nil {
			cmd.slot = cmdSlot(cmd.commandName, cmd.args)
			slots[i] = cmd.slot
		}
	}
	addrs, err := p.c.getAddrForSlots(slots, p.readOnly)
	if err != nil {
		return err
	}
	if len(addrs) != len(p.cmds) {
		return errors.New("addr count didn't match cmd count")
	}
	p.batches = make(map[string]*batch)
	for i := range p.cmds {
		addr := addrs[i]
		if len(addr) > 0 {
			p.cmds[i].addr = addr
			bt, exist := p.batches[addr]
			if !exist {
				bt = &batch{
					addr: addr,
				}
				p.batches[addr] = bt
			}
			bt.cmds = append(bt.cmds, p.cmds[i])
		}
	}
	return nil
}

// build the redirect batches to handling MOVED error
func (p *pipeLiner) buildRedirectBatches() int {
	// clear all batches commands
	for _, bt := range p.batches {
		bt.cmds = nil
	}
	redir_count := 0
	for _, cmd := range p.cmds {
		if cmd != nil && cmd.re != nil {
			addr := cmd.re.Addr
			bt, exist := p.batches[addr]
			if !exist || bt == nil {
				bt = &batch{
					addr: addr,
				}
				p.batches[addr] = bt
			}
			bt.cmds = append(bt.cmds, cmd)
			redir_count++
		}
	}
	return redir_count
}

func (p *pipeLiner) doRedirect() {
	redir_count := p.buildRedirectBatches()
	if redir_count > 0 {
		p.runBatches()
	}
}

// run all the batches in goroutines, and wait them returning
func (p *pipeLiner) runBatches() {
	if len(p.batches) <= 0 {
		return
	}
	var wg sync.WaitGroup
	for _, bt := range p.batches {
		if bt != nil && len(bt.cmds) > 0 {
			wg.Add(1)
			go func(b *batch) {
				defer wg.Done()
				err := b.run(p)
				if err != nil {
					return
				}
			}(bt)
		}
	}
	wg.Wait()
}

/* redis.Conn interface impl*/

func (p *pipeLiner) Err() error {
	return nil
}

func (p *pipeLiner) Do(commandName string, args ...interface{}) (interface{}, error) {
	var err error
	if p.connForDo == nil {
		p.connForDo, err = RetryConn(p.c.Get(), 3, 100*time.Millisecond)
		if err != nil {
			return nil, err
		}
	}
	if p.connForDo != nil {
		return p.connForDo.Do(commandName, args...)
	}
	return nil, errors.New("Get conn failed")
}

// Just append the command in the p.cmds
func (p *pipeLiner) Send(commandName string, args ...interface{}) error {
	p.cmds = append(p.cmds, &cmd{
		commandName: commandName,
		args:        args,
	})
	return nil
}

func (p *pipeLiner) Close() error {
	if p.connForDo != nil {
		p.connForDo.Close()
	}
	for _, bt := range p.batches {
		if bt != nil && bt.conn != nil {
			bt.conn.Close()
		}
	}
	return nil
}

// Build all the batches, and run them concurrently in different goroutines.
// All replies will be stored in every cmd struct once all requests respond.
// The redirection will be handled if there is any MOVED error returned.
func (p *pipeLiner) Flush() error {
	var err error
	if p.flushed {
		return nil
	}
	err = p.buildBatches()
	if err != nil {
		return err
	}
	p.runBatches()
	p.doRedirect()
	p.flushed = true
	return nil
}

// Receive a reply for pipeline
// All replies are received when flush invoked.
// recvPos is a cursor to simulate a real Receive, just like what a real redis.Conn does
func (p *pipeLiner) Receive() (reply interface{}, err error) {
	if !p.flushed {
		return nil, errors.New("need to flush before receive")
	}
	if len(p.cmds) <= 0 {
		return nil, nil
	}
	p.recvPos++
	if p.recvPos >= len(p.cmds) || p.recvPos < 0 {
		return nil, errors.New("no more reply")
	}
	reply = p.cmds[p.recvPos].reply
	err = p.cmds[p.recvPos].reply_err
	return
}
