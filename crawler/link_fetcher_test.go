package crawler

import (
	"context"

	"github.com/brandonshearin/ask_brandon/crawler/mocks"
	"github.com/golang/mock/gomock"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(LinkFetcherTestSuite))

type LinkFetcherTestSuite struct {
	urlGetter       *mocks.MockURLGetter
	privNetDetector *mocks.MockPrivateNetworkDetector
}

func (s *LinkFetcherTestSuite) SetUpTest(c *gc.C) {
}

func (s *LinkFetcherTestSuite) TestLinkFetcherWithExcludedExtension(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.urlGetter = mocks.NewMockURLGetter(ctrl)
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	p := s.fetchLink(c, "http://example.com/foo.png")
	c.Assert(p, gc.IsNil)
}

func (s *LinkFetcherTestSuite) fetchLink(c *gc.C, url string) *crawlerPayload {
	p := &crawlerPayload{
		URL: url,
	}

	out, err := newLinkFetcher(s.urlGetter, s.privNetDetector).Process(context.TODO(), p)
	c.Assert(err, gc.IsNil)
	if out != nil {
		c.Assert(out, gc.FitsTypeOf, p)
		return out.(*crawlerPayload)
	}

	return nil
}
