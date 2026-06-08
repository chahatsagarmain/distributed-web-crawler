package main

import (
	"log"
	"github.com/chahatsagarmain/distributed-web-crawler/common"
	"github.com/chahatsagarmain/distributed-web-crawler/worker/internal/crawler"
)

var Conn *common.Connections

func main(){
	err := common.InitConfig()
	if err != nil{
		log.Fatalf("ERROR : initializing configuration had an unexpected error %v" , err)
	}
	Conn , err = common.ConnectAll(common.AppConfig)
	if err != nil{
		log.Fatalf("ERROR : connectiong to database or message broker %v" , err)
	}

	crawler := crawler.NewCrawler()
	resp , err := crawler.CrawlUrl("https://open.spotify.com/album/1aGapZGHBovnmhwqVNI6JZ", 0)
	if err != nil{
		log.Printf("ERROR : crawling error %v" , err)
		return
	}
	log.Printf("%v" , resp.NextUrls)
}