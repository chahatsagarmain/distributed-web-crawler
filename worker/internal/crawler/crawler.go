package crawler

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	urlParser "net/url"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/purell"
	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/metrics"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/robots"
	"strconv"
)

type Crawler struct {
	Client     *http.Client
	RobotCheck *robots.RobotChecker
	Politeness *PolitenessManager
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
		Politeness: NewPolitenessManager(context.Background()),
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
	start := time.Now()
	status := "success"
	defer func() {
		metrics.PageProcessingDuration.Observe(time.Since(start).Seconds())
		metrics.PagesProcessedTotal.WithLabelValues(status).Inc()
	}()

	parsedUrl, err := urlParser.Parse(rawUrl)
	if err != nil {
		status = "failure"
		slog.Error("parsing url", "url", rawUrl, "error", err)
		return common.UrlData{}, err
	}
	url := parsedUrl.String()
	url, err = NormalizeURL(url)
	if err != nil {
		status = "failure"
		slog.Error("url can't be normalized", "url", url, "error", err)
		return common.UrlData{}, err
	}

	// Determine politeness delay
	delay, err := c.RobotCheck.GetCrawlDelay(url)
	if err != nil || delay <= 0 {
		delay = time.Duration(common.AppConfig.DefaultPolitenessDelay) * time.Millisecond
	}

	err = c.Politeness.Enforce(context.Background(), parsedUrl.Host, delay)
	if err != nil {
		status = "failure"
		slog.Error("politeness enforcement failed", "host", parsedUrl.Host, "error", err)
		return common.UrlData{}, err
	}

	allowed, err := c.RobotCheck.IsAllowed(url)
	if err != nil {
		status = "failure"
		slog.Error("checking robots.txt", "url", url, "error", err)
		return common.UrlData{}, err
	}
	if !allowed {
		slog.Info("URL is disallowed by robots.txt", "url", url)
		status = "failure"
		return common.UrlData{
			Url:       url,
			HasRobots: true,
		}, fmt.Errorf("URL disallowed by robots.txt")
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		status = "failure"
		slog.Error("creating request", "url", url, "error", err)
		return common.UrlData{}, err
	}
	req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...")
	req.Header.Add("Accept", "application/json")

	client := c.Client
	resp, err := client.Do(req)
	if err != nil {
		status = "failure"
		slog.Error("sending request", "url", url, "error", err)
		return common.UrlData{}, err
	}
	metrics.HTTPStatusCodesTotal.WithLabelValues(strconv.Itoa(resp.StatusCode), parsedUrl.Host).Inc()
	defer resp.Body.Close()
	htmlDocument, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		status = "failure"
		slog.Error("reading html", "url", url, "error", err)
		return common.UrlData{}, err
	}
	rawHtml, err := goquery.OuterHtml(htmlDocument.Selection)
	if err != nil {
		status = "failure"
		slog.Error("reading html", "url", url, "error", err)
		return common.UrlData{}, err
	}
	metrics.BytesDownloadedTotal.Add(float64(len(rawHtml)))

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
