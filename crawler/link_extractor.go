package crawler

import (
	"context"
	"net/url"
	"regexp"

	"github.com/brandonshearin/ask_brandon/pipeline"
)

var (
	/*Use these regular expressions to do the following:
	- skip extracted links that point to non-HTML content
	- locate the <base href="XXX"> tag and capture the value
	- extract links from the HTML contents
	- identify links that should not be considered when calculating pagerank score
	*/
	exclusionRegex = regexp.MustCompile(`(?i)\.(?:jpg|jpeg|png|gif|ico|css|js)$`)
	baseHrefRegex  = regexp.MustCompile(`(?i)<base.*?href\s*?=\s*?"(.*?)\s*?"`)
	findLinkRegex  = regexp.MustCompile(`(?i)<a.*?href\s*?=\s*?"\s*?(.*?)\s*?".*?>`)
	nofollowRegex  = regexp.MustCompile(`(?i)rel\s*?=\s*?"?nofollow"?`)
)

func resolveURL(relTo *url.URL, target string) *url.URL {
	tLen := len(target)
	if tLen == 0 {
		return nil
	} else if tLen >= 1 && target[0] == '/' {
		if tLen >= 2 && target[1] == '/' {
			target = relTo.Scheme + ":" + target
		}
	}
	if targetURL, err := url.Parse(target); err == nil {
		return relTo.ResolveReference(targetURL)
	}

	return nil
}

type linkExtractor struct {
	netDetector PrivateNetworkDetector
}

func newLinkExtractor(netDetector PrivateNetworkDetector) *linkExtractor {
	return &linkExtractor{
		netDetector: netDetector,
	}
}

//Process encapsulates the business logic of the link extractor
func (le *linkExtractor) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {
	payload := p.(*crawlerPayload)
	//in order to qualify any relative link we encounter,
	//we need a fully qualified link to use as a base
	relTo, err := url.Parse(payload.URL)
	if err != nil {
		return nil, err
	}

	// Search page content for a <base> tag and resolve it to an absolute URL
	content := payload.RawContent.String()
	if baseMatch := baseHrefRegex.FindStringSubmatch(content); len(baseMatch) == 2 {
		if base := resolveURL(relTo, ensureHasTrailingSlash(baseMatch[1])); base != nil {
			relTo = base
		}
	}

	seenMap := make(map[string]struct{})
	for _, match := range findLinkRegex.FindAllStringSubmatch(content, -1) {
		link := resolveURL(relTo, match[1])
		if link == nil || !le.retainLink(relTo.Hostname(), link) {
			continue
		}

		link.Fragment = ""
		linkStr := link.String()
		if _, seen := seenMap[linkStr]; seen || exclusionRegex.MatchString(linkStr) {
			continue //skip already seen links and links that do not contain HTML
		}

		seenMap[linkStr] = struct{}{}
		if nofollowRegex.MatchString(match[0]) {
			payload.NoFollowLinks = append(payload.NoFollowLinks, linkStr)
		} else {
			payload.Links = append(payload.Links, linkStr)
		}
	}

	return payload, nil
}

func ensureHasTrailingSlash(s string) string {
	if s[len(s)-1] != '/' {
		return s + "/"
	}
	return s
}

func (le *linkExtractor) retainLink(srcHost string, link *url.URL) bool {
	// Skip links that could not be resolved
	if link == nil {
		return false
	}

	// Skip links with non http(s) schemes
	if link.Scheme != "http" && link.Scheme != "https" {
		return false
	}

	// Keep links to the same host
	if link.Hostname() == srcHost {
		return true
	}

	// Skip links that resolve to private networks
	if isPrivate, err := le.netDetector.IsPrivate(link.Host); err != nil || isPrivate {
		return false
	}

	return true
}
