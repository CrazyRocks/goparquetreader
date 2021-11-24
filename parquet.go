package tools

import (
	"fmt"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/gogf/gf/os/gfile"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/text/gregex"
	"github.com/gogf/gf/text/gstr"
	ch "github.com/leprosus/golang-clickhouse"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type importer struct {
}

var Importer = new(importer)
var path = "E:\\mysql\\"

type Account struct {
}

var (
	user = "default"
	pass = ""
	host = ""
	port = 8123
)

func (c *importer) Import() {
	entries, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	conn := ch.New(host, port, user, pass)
	c.ReadFile("log_event", conn)

	for _, entry := range entries {
		c.ReadFile(entry.Name(), conn)
	}
}

func (c *importer) ReadFile(src string, connect *ch.Conn) {

	tableName := c.GetTableName(src)
	tables := []string{"account", "log_event"}
	inTable := gstr.InArray(tables, tableName)
	if !inTable {
		return
	}
	dirPath := gfile.Join(path, src)
	files, _ := gfile.ScanDirFile(dirPath, "*.gz.parquet", false)
	for _, file := range files {
		c.SendFile(file, connect, tableName)
	}
}
func (c *importer) SendFile(filePath string, connect *ch.Conn, tableName string) {
	r, err := os.Open(filePath)
	if err != nil {
		glog.Debug(err.Error())
		return
	}
	defer r.Close()

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		glog.Debug(err.Error())
		return
	}

	log.Printf("Printing file %s", filePath)
	count := 0

	for {
		if count > 300 {

			time.Sleep(time.Second * 1)
			count = 0
			if err != nil {
				glog.Debug(err.Error())
				continue
			}
		}

		row, err := fr.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			glog.Debug(fmt.Errorf("reading record failed: %w", err))
			continue
		}

		keys := ""
		values := ""
		for k, v := range row {
			if vv, ok := v.([]byte); ok {
				v = string(vv)
			}
			keys = fmt.Sprintf("%v,%v", k, keys)
			values = fmt.Sprintf("'%v',%v", v, values)

		}
		keys = gstr.SubStr(keys, 0, len(keys)-1)
		values = gstr.SubStr(values, 0, len(values)-1)
		query := fmt.Sprintf("INSERT INTO open_platform.%s (%v) VALUES (%v);", tableName, keys, values)

		if query == "" {
			continue
		}
		err = connect.ForcedExec(query)
		if err != nil {
			glog.Debug(err.Error())
			continue
		}
		count++

	}

}
func (c *importer) GetTableName(src string) string {
	strs := gstr.Split(src, ".")
	tableName := strs[1]
	tableName, _ = gregex.ReplaceString("_\\d+", "", tableName)
	return tableName
}
