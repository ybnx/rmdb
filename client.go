package rmdb

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type Client struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	conn   net.Conn
	host   string
	port   int
}

func RunClient() {
	var (
		host string
		port int
	)
	flag.StringVar(&host, "host", "127.0.0.1", "set rmdb server host")
	flag.IntVar(&port, "port", 27999, "set rmdb server port")
	flag.Parse()
	printUsage()
	client, err := newClient(host, port)
	if err != nil {
		logger.Errorf("rmdb connect server failed: %s\n", err)
		return
	}
	client.wg.Wait()
	logger.Info("rmdb: client close")
}

func newClient(host string, port int) (*Client, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		return nil, err
	}
	logger.Info("rmdb: connect server successfully")
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		host:   host,
		port:   port,
	}
	client.wg.Add(2)
	go client.handleConn()
	go client.heartBeat()
	return client, nil
}

func (c *Client) handleConn() {
	defer func() {
		err := c.conn.Close()
		if err != nil {
			logger.Errorf("rmdb connect close failed: %s\n", err)
		}
		c.wg.Done()
	}()
	buf := make([]byte, 1024)
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-c.ctx.Done():
			logger.Info("rmdb: stop handle connect")
			return
		default:
			fmt.Print(fmt.Sprintf("rmdb %v:%v>", c.host, c.port))
			sql, err := reader.ReadString('\n') //好像能换成io.copy
			if err != nil {
				if err != io.EOF {
					logger.Errorf("rmdb read from console failed: %s\n", err)
				}
				continue
			}
			sql = strings.Trim(sql, "\r\n; ")
			if sql == "" { //改成switch
				continue
			}
			if sql == "clear" {
				cmd := exec.Command("cmd", "/C", "cls")
				cmd.Stdout = os.Stdout
				err = cmd.Run()
				if err != nil {
					logger.Errorf("rmdb clear console failed: %s\n", err)
				}
				continue
			}
			if sql == "exit" {
				logger.Info("rmdb: stop handle connect")
				c.cancel()
				return
			}
			if sql == "stop" {
				logger.Info("rmdb: stop server")
				logger.Info("rmdb: stop handle connect")
				_, err = c.conn.Write([]byte("stop"))
				if err != nil {
					logger.Errorf("write to connect failed: %s, please wait seconds\n", err)
					success := c.tryReConnect()
					if success {
						continue
					} else {
						return
					}
				}
				c.cancel()
				return
			}
			_, err = c.conn.Write([]byte(sql))
			if err != nil {
				logger.Errorf("write to connect failed: %s, please wait seconds\n", err)
				success := c.tryReConnect()
				if success {
					continue
				} else {
					return
				}
			}
			n, err := c.conn.Read(buf)
			if err != nil {
				logger.Errorf("write to connect failed: %s, please wait seconds\n", err)
				success := c.tryReConnect()
				if success {
					continue
				} else {
					return
				}
			}
			fmt.Println(string(buf[:n]))
		}
	}
}

func (c *Client) heartBeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer func() {
		ticker.Stop()
		c.wg.Done()
	}()
	for {
		select {
		case <-c.ctx.Done():
			logger.Info("rmdb: stop heartbeat")
			return
		case <-ticker.C:
			_, err := c.conn.Write([]byte{2, 0, 0, 1})
			if err != nil {
				logger.Errorf("rmdb heartbeat failed: %s, please wait seconds\n", err)
				success := c.tryReConnect()
				if success {
					continue
				} else {
					return
				}
			}
		}
	}
}

func (c *Client) tryReConnect() bool {
	for i := 0; i < 10; i++ {
		logger.Info("rmdb: try reconnect")
		var err error
		c.conn, err = net.Dial("tcp", fmt.Sprintf("%v:%v", c.host, c.port))
		if err != nil {
			logger.Errorf("rmdb reconnect failed: %s, please check your network, maybe server shutdown\n", err)
			continue
		}
		logger.Info("rmdb: reconnect successfully")
		fmt.Print(fmt.Sprintf("rmdb %v:%v>", c.host, c.port))
		return true
	}
	logger.Error("rmdb: connect timeout, please ctrl+c reboot client")
	c.cancel()
	return false
}
