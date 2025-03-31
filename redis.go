package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

type Command struct {
	Name string
	Args []string
}

type RedisStore struct {
	data  map[string]string
	mutex sync.RWMutex
}

func NewRedisStore() (*RedisStore, error) {
	return &RedisStore{
		data: make(map[string]string),
	}, nil
}

func (r *RedisStore) Get(key string) (string, bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	val, exists := r.data[key]
	if !exists {
		return "", false
	}
	return val, true
}

func (r *RedisStore) Set(key string, val string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.data[key] = val
}

func parseCommand(input string) Command {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return Command{}
	}
	return Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}
}
func handleConnection(conn net.Conn, rs *RedisStore) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := parseCommand(scanner.Text())
		response := processCommand(command, rs)
		conn.Write([]byte(response + "\n"))
	}

}

func StartServer(rs *RedisStore) error {
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Printf("server started on %s\n", listener.Addr())
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("connection error: ", err)
			continue
		}
		go handleConnection(conn, rs)
	}
}

// GET name
func processCommand(cmd Command, rs *RedisStore) string {
	switch cmd.Name {
	case "GET":
		if len(cmd.Args) == 1 {
			val, exists := rs.Get(cmd.Args[0])
			if exists {
				return val
			}
			return "nil"
		}
	case "SET":
		if len(cmd.Args) >= 2 {
			rs.Set(cmd.Args[0], cmd.Args[1])
			return "OK"
		}
	}
	return ""
}
func main() {
	rs, err := NewRedisStore()
	if err != nil {
		log.Fatal(err)
		return
	}
	go func() {
		if err := StartServer(rs); err != nil {
			log.Fatal(err)
		}
	}()
	// input -> redis store.
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) == 0 {
			continue
		}
		command := strings.ToUpper(parts[0])
		args := parts[1:]
		cmd := Command{Name: command, Args: args}
		response := processCommand(cmd, rs)
		fmt.Println(response)

		if err := scanner.Err(); err != nil {
			fmt.Println("error reading input: ", err)
		}

	}

}
