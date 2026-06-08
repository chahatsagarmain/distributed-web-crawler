package crawler

import (
	"fmt"
	"log"
	"net/http"
	urlParser "net/url"

	"github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/purell"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/robots"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/models"
)

type Crawler struct {
	Client     *http.Client
	RobotCheck *robots.RobotChecker
}

func NewCrawler() *Crawler {
	return &Crawler{
		Client:     &http.Client{},
		RobotCheck: robots.NewRobotChecker(),
	}
}

func NormalizeURL(rawURL string) (string, error) {
	// Start with the standard safe flag set
	flags := purell.FlagsSafe |
		purell.FlagRemoveFragment | // Ignore browser-side fragments
		purell.FlagSortQuery | // Order query params alphabetically
		purell.FlagRemoveDuplicateSlashes // Merge double slashes

	// Normalize URL
	normalized, err := purell.NormalizeURLString(rawURL, flags)
	if err != nil {
		return "", err
	}
	return normalized, nil
}

func (c *Crawler) CrawlUrl(rawUrl string, depth int) (models.UrlData, error) {
	parsedUrl, err := urlParser.Parse(rawUrl)
	if err != nil {
		log.Printf("ERROR : parsing url %v , %v\n", rawUrl, err)
		return models.UrlData{}, err
	}
	url := parsedUrl.String()
	url, err = NormalizeURL(url)
	if err != nil {
		fmt.Printf("ERROR : url %v can't be normalized %v", url, err)
		return models.UrlData{}, err
	}
	// Check robots.txt restrictions before crawling
	allowed, err := c.RobotCheck.IsAllowed(url)
	if err != nil {
		log.Printf("ERROR : checking robots.txt for %s: %v\n", url, err)
		return models.UrlData{}, err
	}
	if !allowed {
		log.Printf("INFO : URL %s is disallowed by robots.txt\n", url)
		return models.UrlData{
			Url:       url,
			HasRobots: true,
		}, fmt.Errorf("URL disallowed by robots.txt")
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("ERROR : creating request to %s: %v\n", url, err)
		return models.UrlData{}, err
	}
	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...")
	req.Header.Add("Accept", "application/json")

	client := c.Client
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("ERROR : sending request to %s: %v\n", url, err)
		return models.UrlData{}, err
	}
	defer resp.Body.Close()
	htmlDocument, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Printf("ERROR : reading html %v: %v\n", url, err)
		return models.UrlData{}, err
	}
	rawHtml, err := goquery.OuterHtml(htmlDocument.Selection)
	if err != nil {
		log.Printf("ERROR : reading html %v: %v\n", url, err)
		return models.UrlData{}, err
	}

	var nextUrls []string
	htmlDocument.Find("a").Each(func(index int, item *goquery.Selection) {
		href, exists := item.Attr("href")
		if !exists {
			return
		}
		parsedHref, err := urlParser.Parse(href)
		if err != nil {
			log.Printf("ERROR : parsing extracted href %s: %v\n", href, err)
			return
		}

		resolvedHref := parsedUrl.ResolveReference(parsedHref)
		href = resolvedHref.String()
		href, err = NormalizeURL(href)
		if err != nil {
			log.Printf("ERROR : normalizing url %v: %v\n", href, err)
			return
		}

		nextUrls = append(nextUrls, href)
	})

	return models.UrlData{
		RawHtml:   rawHtml,
		Url:       url,
		NextUrls:  nextUrls,
		HasRobots: false,
		Depth:     depth,
	}, nil
}
