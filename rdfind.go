package main

import (
    "os"
    "fmt"
    "path/filepath"
    "crypto/sha512"
    "io"
    "io/ioutil"
    "encoding/hex"
    "sync"
    "strconv"
    "strings"
)

const (
    separator = string(os.PathSeparator)
    version = "v1.2"
)

type FileInfos struct {
    size int64
    fpath string
}

var filestorage map[string][]FileInfos = make(map[string][]FileInfos)
var mutex sync.Mutex

func showhelp () {
    fmt.Println("program version: ", version)
    fmt.Println("-d folder")
    fmt.Println("-t thread number, default 4")
    fmt.Println("-w handle way:")
    fmt.Println("    0.show same file (default)")
    fmt.Println("    1.delete new find file")
    fmt.Println("    2.change new find file to a hard link")
    fmt.Println("    3.change new find file to a symlink, need super permission")
    fmt.Println("-c channel deepness, default 65536")
    fmt.Println("-e delete empty folder")
    fmt.Println("-v show program version")
}

func gethash(fpath string) (int64, string, error) {
    file, err := os.Open(fpath)
    if err != nil {
        return 0, "", err
    }
    defer file.Close()
    h_ob := sha512.New()
    size, err2 := io.Copy(h_ob, file)
    if err2 != nil {
        return 0, "", err2
    }
    hash := h_ob.Sum(nil)
    hashvalue := hex.EncodeToString(hash)
    return size, hashvalue, nil
}

func removeemptyfolder (fpath string) {
	dir, err := ioutil.ReadDir(fpath)
	if err != nil {
		fmt.Println(err)
		return
	}
	if len(dir) == 0 {
		err2 := os.Remove(fpath)
		if err2 != nil {
			fmt.Println(err2)
		}
		index := strings.LastIndex(fpath, separator)
		path := fpath[:index]
		removefolder (path)
	}
}

func run (ch chan string, wg *sync.WaitGroup, way int, removeempty bool) {
    for fpath := range ch {
        fmt.Println("start check file:", fpath)
        size, key, err := gethash(fpath)
        if err != nil {
            fmt.Println (err)
            continue
        }
        mutex.Lock()
        val, ok := filestorage[key]
        if ok { // key存在，追加
            findsamefile := false
            vallen := len(val)
            for i :=0 ; i < vallen ; i++ {
                if (val[i].size == size) { // 文件大小相同
                    fmt.Println("find a same file", val[i].fpath)
                    findsamefile = true
                }
            }
            if findsamefile { // 找到其他文件大小与sha512均相同的文件
                if way == 0 { // 什么都不做
                    filestorage[key] = append(val, FileInfos{size, fpath})
                } else if way == 1 { // 删除新发现的
                    fmt.Println("delete file", fpath)
                    err2 := os.Remove(fpath)
                    if err2 != nil {
                        fmt.Println(err2)
                    }
                    if removeempty {
                        index := strings.LastIndex(fpath, separator)
                        path := fpath[:index]
                        removeemptyfolder(path)
                    }
                } else if way == 2 { // 删除新发现的，然后创建硬链接
                    filestorage[key] = append(val, FileInfos{size, fpath})
                    fmt.Println("delete file and create a hard link", fpath)
                    err2 := os.Remove(fpath)
                    if err2 != nil {
                        fmt.Println(err2)
                    }
                    err3 := os.Link(val[0].fpath, fpath)
                    if err3 != nil {
                        fmt.Println(err3)
                    }
                } else if way == 3 { // 删除新发现的，然后创建软链接
                    filestorage[key] = append(val, FileInfos{size, fpath})
                    fmt.Println("delete file and create a symlink", fpath)
                    err2 := os.Remove(fpath)
                    if err2 != nil {
                        fmt.Println(err2)
                    }
                    err3 := os.Symlink(val[0].fpath, fpath)
                    if err3 != nil {
                        fmt.Println(err3)
                    }
                }
            } else { // 未找到其他文件大小与sha512均相同的文件
                filestorage[key] = append(val, FileInfos{size, fpath})
            }
        } else { // key不存在，新建
            filestorage[key] = []FileInfos{{size, fpath}}
        }
        mutex.Unlock()
        wg.Done()
    }
}

func main () {
    folder := ""
    threadnum := 4
    way := 0
    chanlen := 65536
    removeempty := false
    args := os.Args
    if args == nil {
        showhelp ()
        return
    }
    argslen := len(args)
    for i := 1 ; i < argslen ; i++ {
        if args[i] == "-d" {
            i++
            folder = args[i]
        } else if args[i] == "-t" {
            i++
            val, err := strconv.Atoi(args[i])
            if err != nil {
                showhelp()
                return
            }
            threadnum = val
        } else if args[i] == "-w" {
            i++
            val, err := strconv.Atoi(args[i])
            if err != nil {
                showhelp()
                return
            }
            way = val
        } else if args[i] == "-c" {
            i++
            val, err := strconv.Atoi(args[i])
            if err != nil {
                showhelp()
                return
            }
            way = val
        } else if args[i] == "-e" {
            removeempty = true
        } else if args[i] == "-v" || args[i] == "--version" {
            fmt.Println(version)
            return
        } else if args[i] == "-h" || args[i] == "--help" {
            showhelp()
            return
        }
    }
    if folder == "" {
        showhelp()
        return
    }
    wg := sync.WaitGroup{}
    ch := make(chan string, chanlen)
    defer close(ch)
    for i := 0 ; i < threadnum ; i++ { // 同时启动threadnum个协程
        go run(ch, &wg, way, removeempty)
    }
    filepath.Walk(folder, func (path string, info os.FileInfo, err error) error {
        if err != nil {
            fmt.Println(err)
            return nil
        }
        if info.IsDir() == false { // 只有是普通文件才计算与判断
            wg.Add(1)
            ch <- path
        } else if removeempty {
            removeemptyfolder(path)
        }
        return nil
    })
    wg.Wait()
}
