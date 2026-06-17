package robots

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
)

// RobotChecker handles robots.txt fetching, parsing, caching, and rule verification.
type RobotChecker struct {
	Client    *http.Client
	UserAgent string
	cache     map[string]*robotstxt.Group
	cacheMu   sync.RWMutex
}

func NewRobotChecker() *RobotChecker {
	return &RobotChecker{
		Client: &http.Client{
			Timeout: 5 * time.Second,
		},
		UserAgent: "*",
		cache:     make(map[string]*robotstxt.Group),
	}
}

func (rc *RobotChecker) IsAllowed(targetURL string) (bool, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return false, fmt.Errorf("failed to parse target URL: %w", err)
	}

	host := parsedURL.Host
	if host == "" {
		return false, fmt.Errorf("empty host in URL: %s", targetURL)
	}

	// Check cache first
	rc.cacheMu.RLock()
	group, cached := rc.cache[host]
	rc.cacheMu.RUnlock()

	if cached {
		if group == nil {
			// Cached nil means robots.txt doesn't exist or allows all
			return true, nil
		}
		return group.Test(parsedURL.Path), nil
	}

	// Cache miss: fetch and parse robots.txt
	robotsURL := fmt.Sprintf("%s://%s/robots.txt", parsedURL.Scheme, parsedURL.Host)
	req, err := http.NewRequest("GET", robotsURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", rc.UserAgent)

	resp, err := rc.Client.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to fetch robots.txt: %w", err)
	}
	defer resp.Body.Close()

	var robotsGroup *robotstxt.Group

	switch {
	case resp.StatusCode == http.StatusNotFound:
		robotsGroup = nil

	case resp.StatusCode >= 400 && resp.StatusCode < 500:

		robotsGroup = nil

	case resp.StatusCode >= 500:
		return false, fmt.Errorf("temporary server error fetching robots.txt: status code %d", resp.StatusCode)

	case resp.StatusCode == http.StatusOK:
		robotsData, err := robotstxt.FromResponse(resp)
		if err != nil {
			// Malformed robots.txt: default to allow all
			robotsGroup = nil
		} else {
			robotsGroup = robotsData.FindGroup(rc.UserAgent)
		}

	default:
		return false, fmt.Errorf("unexpected status code fetching robots.txt: %d", resp.StatusCode)
	}

	// Cache the result
	rc.cacheMu.Lock()
	rc.cache[host] = robotsGroup
	rc.cacheMu.Unlock()

	if robotsGroup == nil {
		return true, nil
	}
	return robotsGroup.Test(parsedURL.Path), nil
}

// GetCrawlDelay returns the parsed crawl delay for a given URL, or 0 if not specified.
func (rc *RobotChecker) GetCrawlDelay(targetURL string) (time.Duration, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return 0, fmt.Errorf("failed to parse target URL: %w", err)
	}

	host := parsedURL.Host
	if host == "" {
		return 0, fmt.Errorf("empty host in URL: %s", targetURL)
	}

	// Check cache first
	rc.cacheMu.RLock()
	group, cached := rc.cache[host]
	rc.cacheMu.RUnlock()

	// If not cached, fetch robots.txt first to populate the cache
	if !cached {
		_, err = rc.IsAllowed(targetURL)
		if err != nil {
			return 0, err
		}
		rc.cacheMu.RLock()
		group = rc.cache[host]
		rc.cacheMu.RUnlock()
	}

	if group == nil {
		return 0, nil
	}
	return group.CrawlDelay, nil
}
