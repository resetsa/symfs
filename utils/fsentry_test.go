package utils

import (
	"os"
	"strings"
	"testing"

	"github.com/gocql/gocql"
)

var (
	prefixVids = [10]string{
		"vol-01",
		"volume-0001",
		"volume-0002",
		"volume-0003",
		"volume",
		"volumeX",
		"volumeY",
		"",
		"vol",
		"scan",
	}
	prefixUrls = [10]string{
		"https://",
		"http://test/",
		"http://testhost",
		"http://testhost10",
		"",
		"http://testjost/",
		"http://localhost/",
		"http://localhost/10",
		"http://localhost10/",
		"scan",
	}
)

func TestFsEntryString(t *testing.T) {
	entry := FsEntry{
		vid:      "10",
		archived: true,
		deleted:  false,
		url:      "https://testhost/10",
	}
	result := entry.String()
	expect_result := "10,true,false,https://testhost/10"
	if expect_result != result {
		t.Errorf("got %q, wanted %q", result, expect_result)
	}
}

func CheckNewFsEntry(t *testing.T, prefixVid string, prefixUrl string) {
	fsEntry := NewFsEntry(prefixVid, prefixUrl, false, false)
	result := fsEntry.String()
	t.Log(result)
	if !strings.Contains(fsEntry.vid, prefixVid) {
		t.Errorf("Value got %q not contains %q", fsEntry.vid, prefixVid)
	}
	if !strings.Contains(fsEntry.url, prefixUrl) {
		t.Errorf("Value got %q not contains %q", fsEntry.url, prefixUrl)
	}
	if _, err := gocql.ParseUUID(strings.Split(fsEntry.vid, "/")[1]); err != nil {
		t.Errorf("Value got %q not UUID", fsEntry.vid)
	}
}

func TestNewFsEntry(t *testing.T) {
	for count, pVid := range prefixVids {
		pUrl := prefixUrls[count]
		CheckNewFsEntry(t, pVid, pUrl)
	}
}

func TestGenerateData(t *testing.T) {
	prefixVid := "vg-000001"
	prefixUrl := "http://localhost"
	filename := "./testdata.csv"
	count := 1000
	if _, err := os.Stat(filename); err == nil {
		os.Remove(filename)
	}
	if err := GenerateData(filename, uint32(count), prefixVid, prefixUrl); err != nil {
		t.Errorf("Error on %s", err)
	}
}

// func TestReadFromFile(t *testing.T) {
// 	filepath := "../generated.csv"
// 	delim := ","
// 	if _, err := ReadFromFile(filepath, delim); err != nil {
// 		t.Error(err)
// 	}
// }

func TestNewFsEntryFromString(t *testing.T) {
	strValue := "vg-000001/56499193-4570-11ee-871e-0019d14ccba6,false,false,https://localhost//2023/08/28/09/58/4856499193-4570-11ee-871e-0019d14ccba6"
	if entry, err := NewFsEntryFromString(strValue, ","); err != nil {
		t.Error(err)
	} else {
		t.Log(entry)
	}
}
