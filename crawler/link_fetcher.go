package crawler

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/brandonshearin/ask_brandon/pipeline"
)

type linkFetcher struct {
	urlGetter   URLGetter
	netDetector PrivateNetworkDetector
}

//URLGetter is implmented by objects that can perform HTTP GET requests
type URLGetter interface {
	Get(url string) (*http.Response, error)
}

//PrivateNetworkDetector is implemented by objects that can detect whether a host
//resolves to a private network address
type PrivateNetworkDetector interface {
	IsPrivate(host string) (bool, error)
}

func newLinkFetcher(urlGetter URLGetter, netDetector PrivateNetworkDetector) *linkFetcher {
	return &linkFetcher{
		netDetector: netDetector,
		urlGetter:   urlGetter,
	}
}

func (lf *linkFetcher) Process(
	ctx context.Context,
	p pipeline.Payload,
) (pipeline.Payload, error) {

	payload := p.(*crawlerPayload)

	//check the URL against a case-insensitive regex designed to
	//match file extensions that are known to contain binary data
	//or text content (images, loadable scripts, JSON, etc..)
	if exclusionRegex.MatchString(payload.URL) {
		return nil, nil
	}

	//second pre-check: ensures crawler ignores URLs that resolve to private network addresses
	if isPrivate, err := lf.isPrivate(payload.URL); err != nil || isPrivate {
		return nil, nil //don't crawl links in private networks
	}

	res, err := lf.urlGetter.Get(payload.URL)
	if err != nil {
		return nil, nil
	}

	//for GET requests that complete w/o error, copy the response
	//body into the payload's raw content field, then close
	//body to avoid memory leaks
	_, err = io.Copy(&payload.RawContent, res.Body)
	_ = res.Body.Close()
	if err != nil {
		return nil, err
	}

	//Sanity check #1- if status code not in 2xx range, discard the payload
	//rather than returning an error, as the latter would cause the pipeline to
	//terminate.  Not processing a link is not a big issue
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, nil
	}

	//Sanity check #2- content type header should indicate an html document, otherwise
	//there is no point in further processing
	if contentType := res.Header.Get("Content-Type"); !strings.Contains(contentType, "html") {
		return nil, nil
	}
	return nil, nil
}

func (lf *linkFetcher) isPrivate(URL string) (bool, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return false, err
	}
	return lf.netDetector.IsPrivate(u.Hostname())
}
