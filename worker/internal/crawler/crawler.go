package crawler

import (
	"fmt"
	"log/slog"
	"net/http"
	urlParser "net/url"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/purell"
	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/robots"
)

type Crawler struct {
	Client     *http.Client
	RobotCheck *robots.RobotChecker
}

func NewCrawler() *Crawler {
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Crawler{
		Client: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
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

func (c *Crawler) CrawlUrl(rawUrl string, depth int) (common.UrlData, error) {
	parsedUrl, err := urlParser.Parse(rawUrl)
	if err != nil {
		slog.Error("parsing url", "url", rawUrl, "error", err)
		return common.UrlData{}, err
	}
	url := parsedUrl.String()
	url, err = NormalizeURL(url)
	if err != nil {
		slog.Error("url can't be normalized", "url", url, "error", err)
		return common.UrlData{}, err
	}
	// Check robots.txt restrictions before crawling
	allowed, err := c.RobotCheck.IsAllowed(url)
	if err != nil {
		slog.Error("checking robots.txt", "url", url, "error", err)
		return common.UrlData{}, err
	}
	if !allowed {
		slog.Info("URL is disallowed by robots.txt", "url", url)
		return common.UrlData{
			Url:       url,
			HasRobots: true,
		}, fmt.Errorf("URL disallowed by robots.txt")
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		slog.Error("creating request", "url", url, "error", err)
		return common.UrlData{}, err
	}
	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...")
	req.Header.Add("Accept", "application/json")

	client := c.Client
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("sending request", "url", url, "error", err)
		return common.UrlData{}, err
	}
	defer resp.Body.Close()
	htmlDocument, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		slog.Error("reading html", "url", url, "error", err)
		return common.UrlData{}, err
	}
	rawHtml, err := goquery.OuterHtml(htmlDocument.Selection)
	if err != nil {
		slog.Error("reading html", "url", url, "error", err)
		return common.UrlData{}, err
	}

	var nextUrls []string
	htmlDocument.Find("a").Each(func(index int, item *goquery.Selection) {
		href, exists := item.Attr("href")
		if !exists {
			return
		}
		parsedHref, err := urlParser.Parse(href)
		if err != nil {
			slog.Error("parsing extracted href", "href", href, "error", err)
			return
		}

		resolvedHref := parsedUrl.ResolveReference(parsedHref)
		href = resolvedHref.String()
		href, err = NormalizeURL(href)
		if err != nil {
			slog.Error("normalizing url", "url", href, "error", err)
			return
		}

		nextUrls = append(nextUrls, href)
	})

	return common.UrlData{
		RawHtml:   rawHtml,
		Url:       url,
		NextUrls:  nextUrls,
		HasRobots: false,
		Depth:     depth,
	}, nil
}
