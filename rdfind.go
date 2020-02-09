package main

import (
    "os"
    "fmt"
    "path/filepath"
    "crypto/sha512"
    "io"
    "encoding/hex"
    "sync"
    "strconv"
)

var filestorage map[string][]string = make (map[string][]string)
var mutex sync.Mutex

func showhelp () {
    fmt.Println("-d folder")
    fmt.Println("-t thread number, default 10")
    fmt.Println("-w handle way:")
    fmt.Println("    0.show same file (default)")
    fmt.Println("    1.delete new find file")
    fmt.Println("    2.change new find file to a hard link")
    fmt.Println("    3.change new find file to a symlink, need super permission")
}

func gethash(filepath string) string {
    file, err := os.Open(filepath)
    if err != nil {
        fmt.Printf("failed to open %s\n", filepath)
        return ""
    }
    defer file.Close()
    h_ob := sha512.New()
    _, err2 := io.Copy(h_ob, file)
    if err2 != nil {
        fmt.Println("something wrong when use sha512 interface...")
        return ""
    }
    hash := h_ob.Sum(nil)
    hashvalue := hex.EncodeToString(hash)
    return hashvalue
}

func run (ch chan string, wg *sync.WaitGroup, way int) {
    for filepath := range ch {
        key := gethash(filepath)
        mutex.Lock()
        val, ok := filestorage[key]
        if ok { // key存在，追加
            arr := append(val, filepath)
            fmt.Println("find a same file", arr)
            if way == 0 { // 什么都不做
                filestorage[key] = arr
            } else if way == 1 { // 删除新发现的
                fmt.Println("delete file", filepath)
                os.Remove(filepath)
            } else if way == 2 { // 删除新发现的，然后创建硬链接
                filestorage[key] = arr
                fmt.Println("delete file and create a hard link", filepath)
                os.Remove(filepath)
                os.Link(val[0], filepath)
            } else if way == 3 { // 删除新发现的，然后创建软链接
                filestorage[key] = arr
                fmt.Println("delete file and create a symlink", filepath)
                os.Remove(filepath)
                os.Symlink(val[0], filepath)
            }
        } else { // key不存在，新建
            val2 := []string{filepath}
            filestorage[key] = val2
        }
        mutex.Unlock()
        wg.Done()
    }
}

func main () {
    folder := ""
    threadnum := 10
    way := 0
    args := os.Args
    if args == nil {
        showhelp ()
        return
    }
    argslen := len (args)
    for i := 1 ; i < argslen ; i++ {
        if args[i] == "-d" {
            i++
            folder = args[i]
        }
        if args[i] == "-t" {
            i++
            val, err := strconv.Atoi(args[i])
            if err != nil {
                showhelp ()
                return
            }
            threadnum = val
        }
        if args[i] == "-w" {
            i++
            val, err := strconv.Atoi(args[i])
            if err != nil {
                showhelp ()
                return
            }
            way = val
        }
        if args[i] == "-h" || args[i] == "--help" {
            showhelp ()
            return
        }
    }
    if folder == "" {
        showhelp ()
        return
    }
    wg := sync.WaitGroup{}
    ch := make(chan string, 65536)
    defer close(ch)
    for i := 0 ; i < threadnum ; i++ { // 同时启动10个协程
        go run (ch, &wg, way)
    }
    filepath.Walk(folder, func (path string, info os.FileInfo, err error) error {
        if err != nil {
            fmt.Println(err)
            return nil
        }
        if info.IsDir() == false { // 只有是普通文件才计算
            wg.Add(1)
            ch <- path
        }
        return nil
    })
    wg.Wait()
}
