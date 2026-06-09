package common

type CrawlMessage struct {
	URL          string `json:"url"`
	CurrentDepth int    `json:"current_depth"`
	MaxDepth     int    `json:"max_depth"`
}

type CrawlDocument struct {
	Url       string   `bson:"url"`
	RawHtml   string   `bson:"raw_html"`
	NextUrls  []string `bson:"-"` // commented out for no db save
	HasRobots bool     `bson:"has_robots"`
	Depth     int      `bson:"depth"`
}

type UrlData struct {
	Url       string   `json:"url" bson:"url"`
	RawHtml   string   `json:"raw_html" bson:"raw_html"`
	NextUrls  []string `json:"next_urls" bson:"-"` // commented out for no db save 
	HasRobots bool     `json:"has_robots" bson:"has_robots"`
	Depth     int      `json:"depth" bson:"depth"`
}
