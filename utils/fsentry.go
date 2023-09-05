package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/onokonem/sillyQueueServer/timeuuid"
)

type FsEntry struct {
	vid      string
	archived bool
	deleted  bool
	url      string
}

func (F FsEntry) String() string {
	return fmt.Sprintf("%s,%t,%t,%s", F.vid, F.archived, F.deleted, F.url)
}

func NewFsEntry(prefixVid, prefixUrl string, archived, deleted bool) FsEntry {
	var entry FsEntry
	timeUuid := timeuuid.TimeUUID()
	entry.vid = fmt.Sprintf("%s/%s", prefixVid, timeUuid.String())
	entry.archived = archived
	entry.deleted = deleted
	currentTime := time.Now()
	timePartUrl := currentTime.Format("2006/01/02/15/04/05")
	entry.url = fmt.Sprintf("%s/%s%s", prefixUrl, timePartUrl, timeUuid.String())
	return entry
}

func GenerateData(filename string, count uint32, prefixVid, prefixUrl string) error {
	var i uint32
	fo, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fo.Close()
	for i = 0; i < count; i++ {
		f := NewFsEntry(prefixVid, prefixUrl, false, false)
		if _, err := io.WriteString(fo, fmt.Sprintf("%s\n", f.String())); err != nil {
			return err
		}
	}
	return nil
}

func ReadFromFile(filename string, delim string) ([]FsEntry, error) {
	var result []FsEntry
	file, err := os.Open(filename)
	if err != nil {
		return result, err
	}
	defer file.Close()
	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		if entry, err := NewFsEntryFromString(fileScanner.Text(), delim); err != nil {
			return result, err
		} else {
			result = append(result, entry)
		}
	}
	return result, nil
}

func NewFsEntryFromString(strVal string, delim string) (FsEntry, error) {
	nullFsEntry := FsEntry{
		vid:      "",
		url:      "",
		archived: false,
		deleted:  false,
	}
	entryCsv := strings.Split(strVal, delim)
	if len(entryCsv) != GetAttrNumber(nullFsEntry) {
		return nullFsEntry, fmt.Errorf("split string not contains %d part", GetAttrNumber(nullFsEntry))
	}
	vid := entryCsv[0]
	archived, err_a := strconv.ParseBool(entryCsv[1])
	deleted, err_d := strconv.ParseBool(entryCsv[2])
	switch {
	case err_a != nil:
		return nullFsEntry, err_a
	case err_d != nil:
		return nullFsEntry, err_d
	}
	url := entryCsv[3]
	return FsEntry{
			vid:      vid,
			archived: archived,
			deleted:  deleted,
			url:      url},
		nil
}

func GetAttrNumber(i any) int {
	e := reflect.ValueOf(i)
	return e.NumField()
}
