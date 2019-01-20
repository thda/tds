package netlib

import (
	"fmt"

	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/ianaindex"
)

// utility functions to convert sybase charset names to html charset names.

// maps sybase charset to their htlm/iana names
var nameMap = map[string]string{
	"utf8":     "utf-8",
	"big5":     "big5",
	"big5hk":   "big5",
	"iso_1":    "iso-8859-1",
	"cp874":    "windows-874",
	"cp437":    "cp437",
	"cp850":    "cp850",
	"cp852":    "cp852",
	"cp855":    "cp855",
	"cp857":    "cp857",
	"cp858":    "cp858",
	"cp860":    "cp860",
	"cp864":    "cp864",
	"cp866":    "cp866",
	"cp869":    "cp869",
	"cp932":    "cp932",
	"cp936":    "cp936",
	"cp950":    "cp950",
	"cp1250":   "cp1250",
	"cp1251":   "cp1251",
	"cp1252":   "cp1252",
	"cp1253":   "cp1253",
	"cp1254":   "cp1254",
	"cp1255":   "cp1255",
	"cp1256":   "cp1256",
	"cp1257":   "cp1257",
	"cp1258":   "cp1258",
	"gb18030":  "gb18030",
	"greek8":   "greek8",
	"iso88592": "iso88592",
	"iso88595": "iso88595",
	"iso88596": "iso88596",
	"iso88597": "iso88597",
	"iso88598": "iso88598",
	"iso88599": "iso88599",
	"iso15":    "iso-8859-15",
	"koi8":     "koi8-r",
	"sjis":     "sjis",
	"tis-620":  "tis-620",
	"roman8":   "HPRoman8",
}

var nameToCharset = map[string]encoding.Encoding{}

// fetch the encodings from html aliases and iana index
func init() {
	var ianaidx ianaindex.Index
	for sybName, goName := range nameMap {
		if e, _ := charset.Lookup(goName); e != nil {
			nameToCharset[sybName] = e
			continue
		}
		if e, _ := ianaidx.Encoding(goName); e != nil {
			nameToCharset[sybName] = e
		}
	}
}

func getEncoding(sybName string) (encoding.Encoding, error) {
	if e, found := nameToCharset[sybName]; found {
		return e, nil
	}
	return nil, fmt.Errorf("netlib: unsupported charset: %s", sybName)
}
