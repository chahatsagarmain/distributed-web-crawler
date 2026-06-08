package robots

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRobotChecker_IsAllowed(t *testing.T) {
	tests := []struct {
		name           string
		userAgent      string
		targetPath     string
		robotsResponse string
		statusCode     int
		wantAllowed    bool
		wantErr        bool
	}{
		{
			name:       "Disallowed path for user agent",
			userAgent:  "MyBot",
			targetPath: "/private/data",
			robotsResponse: `User-agent: MyBot
Disallow: /private/
`,
			statusCode:  http.StatusOK,
			wantAllowed: false,
			wantErr:     false,
		},
		{
			name:       "Allowed path for user agent",
			userAgent:  "MyBot",
			targetPath: "/public/data",
			robotsResponse: `User-agent: MyBot
Disallow: /private/
`,
			statusCode:  http.StatusOK,
			wantAllowed: true,
			wantErr:     false,
		},
		{
			name:       "Disallowed path for wildcard, allowed for specific agent",
			userAgent:  "MyBot",
			targetPath: "/private/data",
			robotsResponse: `User-agent: *
Disallow: /private/

User-agent: MyBot
Allow: /private/
`,
			statusCode:  http.StatusOK,
			wantAllowed: true,
			wantErr:     false,
		},
		{
			name:           "404 Not Found response",
			userAgent:      "MyBot",
			targetPath:     "/any/path",
			robotsResponse: "Not Found",
			statusCode:     http.StatusNotFound,
			wantAllowed:    true,
			wantErr:        false,
		},
		{
			name:           "500 Server Error response",
			userAgent:      "MyBot",
			targetPath:     "/any/path",
			robotsResponse: "Internal Server Error",
			statusCode:     http.StatusInternalServerError,
			wantAllowed:    false,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Start a local HTTP server to mock robots.txt
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/robots.txt" {
					t.Errorf("expected request path to be /robots.txt, got %s", r.URL.Path)
				}
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.robotsResponse))
			}))
			defer server.Close()

			rc := NewRobotChecker()
			if tt.userAgent != "" {
				rc.UserAgent = tt.userAgent
			}
			// Target URL is server.URL + targetPath
			targetURL := server.URL + tt.targetPath

			allowed, err := rc.IsAllowed(targetURL)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsAllowed() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if allowed != tt.wantAllowed {
				t.Errorf("IsAllowed() allowed = %v, wantAllowed %v", allowed, tt.wantAllowed)
			}
		})
	}
}

func TestRobotChecker_Cache(t *testing.T) {
	requestsCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("User-agent: *\nDisallow: /secret\n"))
	}))
	defer server.Close()

	rc := NewRobotChecker()

	// First request: should hit server
	allowed, err := rc.IsAllowed(server.URL + "/secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Errorf("expected /secret to be disallowed")
	}
	if requestsCount != 1 {
		t.Errorf("expected 1 request to server, got %d", requestsCount)
	}

	// Second request: should use cache
	allowed, err = rc.IsAllowed(server.URL + "/secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Errorf("expected /secret to be disallowed (cached)")
	}
	if requestsCount != 1 {
		t.Errorf("expected requestsCount to still be 1 (cached), got %d", requestsCount)
	}
}
