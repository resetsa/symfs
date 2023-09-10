package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type FsEntry struct {
	vid      string
	archived bool
	deleted  bool
	url      string
}

func (F *FsEntry) String() string {
	return fmt.Sprintf("%s,%t,%t,%s", F.vid, F.archived, F.deleted, F.url)
}

func NewFsEntry(prefixVid, prefixUrl string, archived, deleted bool) FsEntry {
	var entry FsEntry
	timeUuid := gocql.TimeUUID()
	entry.vid = fmt.Sprintf("%s/%s", prefixVid, timeUuid.String())
	entry.archived = archived
	entry.deleted = deleted
	currentTime := time.Now()
	timePartUrl := currentTime.Format("2006/01/02/15/04/05")
	entry.url = fmt.Sprintf("%s/%s%s", prefixUrl, timePartUrl, timeUuid.String())
	return entry
}

func NewFsEntryFromString(strVal string, delim string) (FsEntry, error) {
	nullFsEntry := FsEntry{}
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
