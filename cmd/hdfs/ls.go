package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/colinmarc/hdfs"
)


func ls(paths []string, long, all, humanReadable bool, jsonFormat bool) {
	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		paths = []string{userDir()}
	}

	files := make([]string, 0, len(paths))
	fileInfos := make([]os.FileInfo, 0, len(paths))
	dirs := make([]string, 0, len(paths))

	for _, p := range paths {
		fi, err := client.Stat(p)
		if err != nil {
			fatal(err)
		}

		if fi.IsDir() {
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
			fileInfos = append(fileInfos, fi)
		}
	}

	if jsonFormat {
		fmt.Print("[")
	}

	if len(files) == 0 && len(dirs) == 1 {
		printDir(client, dirs[0], long, all, humanReadable, jsonFormat, true)
	} else {

		var tw *tabwriter.Writer
		if long {
			tw = lsTabWriter()
			defer tw.Flush()
		}

		first := true

		for i, p := range files {
			if long {
				parentPath := path.Join(p, "..")
				printLong(tw, p, parentPath, fileInfos[i], humanReadable, jsonFormat, first)
			} else {
				printShort(p, jsonFormat, first)
			}
			first = false
		}


		for i, dir := range dirs {
			if i > 0 && !jsonFormat {
				fmt.Println()
			}

			if !jsonFormat {
				fmt.Printf("%s/:\n", dir)
			}
			printDir(client, dir, long, all, humanReadable, jsonFormat, first)
			first = false
		}
	}

	if jsonFormat {
		fmt.Println("\n]")
	}
}

func printDir(client *hdfs.Client, dir string, long, all, humanReadable bool, jsonFormat bool, first bool) {
	dirReader, err := client.Open(dir)
	if err != nil {
		fatal(err)
	}

	var tw *tabwriter.Writer
	if long {
		tw = lsTabWriter()
		defer tw.Flush()
	}

	if all && !jsonFormat {
		if long {
			dirInfo, err := client.Stat(dir)
			if err != nil {
				fatal(err)
			}
			parentPath := path.Join(dir, "..")
			parentInfo, err := client.Stat(parentPath)
			if err != nil {
				fatal(err)
			}

			printLong(tw, ".", parentPath, dirInfo, humanReadable, jsonFormat, first)
			printLong(tw, "..", parentPath, parentInfo, humanReadable, jsonFormat, false)
		} else {
			printShort(".", jsonFormat, first)
			printShort("..", jsonFormat, false)
		}
		first = false
	}

	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			fatal(err)
		}

		printFiles(tw, partial, dir, long, all, humanReadable, jsonFormat, first)
		if first == true && len(partial) > 0 {
			first = false
		}
	}
}

func printFiles(tw *tabwriter.Writer, files []os.FileInfo, parent string, long, all, humanReadable bool, jsonFormat bool, first bool) {
	for _, file := range files {
		if !all && strings.HasPrefix(file.Name(), ".") {
			continue
		}

		if long {
			printLong(tw, file.Name(), parent, file, humanReadable, jsonFormat, first)
		} else {
			fileName := file.Name()
			if jsonFormat {
				fileName = fmt.Sprint(parent, "/", file.Name())
			}
			printShort(fileName, jsonFormat, first)
		}
		first = false
	}
}

func printShort(name string, jsonFormat bool, first bool) {

	if jsonFormat {
		if !first {
			fmt.Printf(",")
		}
		fmt.Printf("\n    { \"name\": \"%s\"}",
			name)
	} else {
		fmt.Println(name)
	}
}

func printLong(tw *tabwriter.Writer, name string, parent string, info os.FileInfo, humanReadable bool, jsonFormat bool, first bool) {
	fi := info.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := strconv.FormatInt(fi.Size(), 10)
	if humanReadable {
		size = formatBytes(uint64(fi.Size()))
	}

	modtime := fi.ModTime()
	date := modtime.Format("Jan _2")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = modtime.Format("2006")
	}

	if jsonFormat {
		if !first {
			fmt.Printf(",")
		}
		fmt.Printf("\n    { \"mode\": \"%s\", \"owner\": \"%s\", \"group\": \"%s\", \"size\": %s, \"modTime\": \"%s\", \"name\": \"%s/%s\"}",
			mode, owner, group, size, modtime, parent, fi.Name())

	} else {
		fmt.Fprintf(tw, "%s \t%s \t %s \t %s \t%s \t%s \t%s\n",
			mode, owner, group, size, date, timeOrYear, name)
	}
}

func lsTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
}
