package models

type UrlData struct {
	Url       string
	RawHtml   string
	NextUrls  []string
	HasRobots bool
	Depth     int
}
