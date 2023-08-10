package rmdb

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

type Server struct {
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	listener net.Listener
	host     string
	port     int
}

func RunServer() {
	var ( //可以改成解析配置文件
		host string
		port int
		//dbPath  string
		//ioMode  int
		//maxPage int
		//maxLine int
	)
	flag.StringVar(&host, "host", "127.0.0.1", "set rmdb server host")
	flag.IntVar(&port, "port", 27999, "set rmdb server port")
	flag.Parse()

	printUsage()
	server, err := NewServer(host, port) //new完必须close
	if err != nil {
		logger.Fatal("rmdb: new server failed: ", err)
		return
	}
	err = server.Listen()
	if err != nil {
		logger.Fatal("rmdb: listen connect failed: ", err)
		return
	}
	logger.Infof("rmdb: server listen on host %v and port %v\n", host, port)
	server.wg.Wait()
	for name, db := range databases {
		err = db.Close()
		if err != nil {
			logger.Errorf("rmdb: close database %s failed: %s\n", name, err)
		}
	}
	logger.Info("rmdb: server shutdown")
}

func printUsage() {
	fmt.Println("                     __  __        \n                    /\\ \\/\\ \\       \n _ __    ___ ___    \\_\\ \\ \\ \\____  \n/\\`'__\\/' __` __`\\  /'_` \\ \\ '__`\\ \n\\ \\ \\/ /\\ \\/\\ \\/\\ \\/\\ \\L\\ \\ \\ \\L\\ \\\n \\ \\_\\ \\ \\_\\ \\_\\ \\_\\ \\___,_\\ \\_,__/\n  \\/_/  \\/_/\\/_/\\/_/\\/__,_ /\\/___/  ")
	fmt.Println("  database which support concurrent transaction")
	fmt.Println("  info                       ------> get rmdb server info")
	fmt.Println("  clear                      ------> clear screen")
	fmt.Println("  exit                       ------> exit rmdb client")
	fmt.Println("  stop                       ------> stop rmdb server and exit rmdb client")
	fmt.Println("  show databases             ------> show databases")
	fmt.Println("  show tables                ------> show tables")
	fmt.Println("  use [name]                 ------> use a database")
	fmt.Println("  select                     ------> query record")
	fmt.Println("  insert                     ------> insert table")
	fmt.Println("  update                     ------> update table")
	fmt.Println("  delete                     ------> delete table")
}

func NewServer(host string, port int) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{
		ctx:    ctx,
		cancel: cancel,
		host:   host,
		port:   port,
	}
	return server, nil
}

func (s *Server) Listen() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", s.host, s.port))
	if err != nil {
		return err
	}
	s.listener = listener
	s.wg.Add(1)
	go s.createConn()
	return nil
}

func (s *Server) createConn() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			logger.Info("rmdb: server stop listener")
			return
		default:
			conn, err := s.listener.Accept() // TODO listener close后还会阻塞在accept吗
			if err != nil {
				logger.Errorf("rmdb server accept listener failed: %s\n", err)
				continue
			}
			s.wg.Add(1)
			go s.handleConn(conn)
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		logger.Info("rmdb: server close connection", conn.RemoteAddr().String())
		err := conn.Close()
		if err != nil {
			logger.Errorf("rmdb connect close failed: %s\n", err)
		}
		s.wg.Done()
	}()
	buf := make([]byte, 1024)
	var db *Database
	var tx *Transaction
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			n, err := conn.Read(buf)
			if err != nil {
				if err != io.EOF { // TODO 为什么client exit这里会eof
					logger.Errorf("rmdb read from connect failed: %s\n", err)
					return
				}
				continue
			}
			if bytes.Equal(buf[:n], []byte{2, 0, 0, 1}) { //heartbeat跳过
				continue
			}
			cmd := strings.Trim(string(buf[:n]), "; ")
			switch cmd {
			case "info":
				var ioMode string
				if GlobalOption.IOMode == 0 { // TODO
					ioMode = "Standard"
				} else if GlobalOption.IOMode == 1 {
					ioMode = "MMapMode"
				} else {
					ioMode = "unknown mode"
				}
				_, err = conn.Write([]byte(fmt.Sprintf("db path: %v\nio mode: %v", GlobalOption.Root, ioMode)))
				if err != nil {
					logger.Errorf("rmdb write to connect failed: %s\n", err)
				}
				continue
			case "stop":
				s.Stop()
				return
			}
			if TrimSpace(cmd) == "showdatabases" {
				res := ShowDatabase()
				_, err = conn.Write([]byte(res + "\nQuery OK"))
				if err != nil {
					logger.Errorf("rmdb write to connect failed: %s\n", err)
				}
				continue
			} else if strings.HasPrefix(cmd, "use") {
				dbName := strings.TrimSpace(strings.Split(cmd, "use")[1])
				newDB, err := UseDatabase(dbName)
				var echo string
				if err == nil {
					if db != nil {
						err = db.Close()
						if err != nil {
							logger.Errorf("rmdb database close failed: %s\n", err)
						}
						db = nil
					}
					db = newDB
					echo = "Query OK"
				} else {
					echo = fmt.Sprintf("use database failed: %s", err)
				}
				_, err = conn.Write([]byte(echo))
				if err != nil {
					logger.Errorf("rmdb write to connect failed: %s\n", err)
				}
				continue
			} else {
				if db == nil {
					_, err = conn.Write([]byte("please use database"))
					if err != nil {
						logger.Errorf("rmdb write to connect failed: %s\n", err)
					}
					continue
				}
				if TrimSpace(cmd) == "showtables" {
					res := db.ShowTables()
					_, err = conn.Write([]byte(res + "\nQuery OK"))
					if err != nil {
						logger.Errorf("rmdb write to connect failed: %s\n", err)
					}
					continue
				} else if strings.HasPrefix(cmd, "insert") || strings.HasPrefix(cmd, "delete") || strings.HasPrefix(cmd, "update") {

					var err error
					if tx == nil {
						err = db.Update(cmd)
					} else {
						err = tx.Update(cmd)
					}

					var echo string
					if err == nil {
						echo = "Query OK"
					} else {
						echo = fmt.Sprintf("update failed: %s", err)
					}
					_, err = conn.Write([]byte(echo))
					if err != nil {
						logger.Errorf("rmdb write to connect failed: %s\n", err)
					}
					continue
				} else if strings.HasPrefix(cmd, "select") {

					var res *ResultSet
					var err error
					if tx == nil {
						res, err = db.Query(cmd)
					} else {
						res, err = tx.Query(cmd)
					}

					var echo string
					if err == nil {
						echo = fmt.Sprint(res.ToString(), "\nQuery OK")
					} else {
						echo = fmt.Sprintf("query failed: %s", err)
					}
					_, err = conn.Write([]byte(echo))
					if err != nil {
						logger.Errorf("rmdb write to connect failed: %s\n", err)
					}
					continue
				} else if cmd == "begin" {
					if tx == nil {
						tx = db.Begin()
						_, err = conn.Write([]byte("Query OK"))
						if err != nil {
							logger.Errorf("rmdb write to connect failed: %s\n", err)
						}
					} else {
						_, err = conn.Write([]byte("please commit previous transaction"))
						if err != nil {
							logger.Errorf("rmdb write to connect failed: %s\n", err)
						}
					}
					continue
				} else if cmd == "commit" {
					if tx == nil {
						_, err = conn.Write([]byte("please create a transaction"))
						if err != nil {
							logger.Errorf("rmdb write to connect failed: %s\n", err)
						}
					} else {
						err = tx.Commit()
						var echo string
						if err == nil {
							tx = nil
							echo = "Query OK"
						} else {
							_ = tx.Rollback()
							tx = nil
							echo = fmt.Sprintf("commit failed: %s", err)
						}
						_, err = conn.Write([]byte(echo))
						if err != nil {
							logger.Errorf("rmdb write to connect failed: %s\n", err)
						}
					}
					continue
				} else if cmd == "rollback" {
					if tx == nil {
						_, err = conn.Write([]byte("please create a transaction"))
						if err != nil {
							logger.Errorf("rmdb write to connect failed: %s\n", err)
						}
					} else {
						err = tx.Rollback()
						var echo string
						if err == nil {
							tx = nil
							echo = "Query OK"
						} else {
							tx = nil
							echo = fmt.Sprintf("rollback failed: %s", err)
						}
						_, err = conn.Write([]byte(echo))
						if err != nil {
							logger.Errorf("rmdb write to connect failed: %s\n", err)
						}
					}
					continue
				} else {
					_, err = conn.Write([]byte("invalid sql"))
					if err != nil {
						logger.Errorf("rmdb write to connect failed: %s\n", err)
					}
					continue
				}
			}
		}
	}
}

func (s *Server) Stop() {
	err := s.listener.Close()
	if err != nil {
		logger.Errorf("rmdb listener close failed: %s\n", err)
	}
	s.cancel() //关闭所有连接的groutine
}
