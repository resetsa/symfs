package utils

import (
	"fmt"
	"io"
	"os"
	"reflect"
)

func GetAttrNumber(i any) int {
	e := reflect.ValueOf(i)
	return e.NumField()
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// generate entry to file
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

// func ReadFromFile(filename string, delim string) ([]FsEntry, error) {
// 	var result []FsEntry
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return result, err
// 	}
// 	defer file.Close()
// 	fileScanner := bufio.NewScanner(file)
// 	fileScanner.Split(bufio.ScanLines)
// 	for fileScanner.Scan() {
// 		if entry, err := NewFsEntryFromString(fileScanner.Text(), delim); err != nil {
// 			return result, err
// 		} else {
// 			result = append(result, entry)
// 		}
// 	}
// 	return result, nil
// }
