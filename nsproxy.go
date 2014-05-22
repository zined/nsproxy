package main

import (
	"fmt"
	"log"
	"net/http"
	"io"
	"errors"
	"flag"
	"strings"
	"math/rand"
	"time"
	"github.com/miekg/dns"
	"github.com/garyburd/redigo/redis"
)

var (
	listenAddr string
	listenPort int
	useRedis bool
	redisHost string
	redisPort int
	nameservers []string
)

func redisGet (c redis.Conn, host string) (string, bool, error) {
	n, err := redis.String(c.Do("GET", host))
	if err != nil {
		return "", false, err
	}

	return n, true, nil
}

func redisSet (c redis.Conn, host string, ip string, ttl uint32) error {
	_, err := c.Do("SET", host, ip, "NX", "EX", ttl)
	if err != nil {
		return err
	}

	return nil
}

func dnsLookup(host string) (string, uint32, error) {

	var (
		result string
		ttl uint32
		hit bool = false
	)

	msg := new(dns.Msg)
	msg.SetQuestion(fmt.Sprintf("%s.", host), dns.TypeA)

	nameserver := fmt.Sprintf("%s:%d", getNameserver(), 53)

	c := new(dns.Client)
	in, _, err := c.Exchange(msg, nameserver)
	if err != nil {
		return "", 0, err
	}

	if t, ok := in.Answer[0].(*dns.A); ok {
		result, ttl = t.A.String(), t.Header().Ttl
		hit = true
	}

	if t, ok := in.Answer[0].(*dns.CNAME); ok {
		result, ttl = t.Target, t.Header().Ttl
		hit = true
	}

	if hit {
		log.Printf("DNS lookup on <%s> succeeded: <%s> => <%s>\n", nameserver, host, result)
		return result, ttl, nil
	} else {
		return "", 0, errors.New("lookup failed.")
	}
}

func lookup (host string) (string, error) {

	var (
		redis_conn redis.Conn
		result string
		err error
		hit bool
		ttl uint32
	)


	if useRedis {
		redis_conn, err = redis.Dial("tcp", ":6379")
		if err != nil {
			log.Fatal("cannot connect to redis: ", err)
		}
		defer redis_conn.Close()

		result, hit, err = redisGet(redis_conn, host)
		if err != nil {
			log.Println("redisGet error: ", err)
		} else if hit {
			log.Printf("Redis hit for Host <%s>: <%s>\n", host, result)
			return result, nil
		}
	}

	result, ttl, err = dnsLookup(host)
	if err != nil {
		return "", err
	}

	if useRedis {
		if err = redisSet(redis_conn, host, result, ttl); err != nil {
			log.Println("redisSet error: ", err)
		}
	}

	return result, nil
}

func internalServerError(w http.ResponseWriter) {
	http.Error(w, "Internal Server Error", http.StatusInternalServerError)
}

func dnsHandlerFunc(w http.ResponseWriter, r *http.Request) {

	url_string := fmt.Sprintf("http://%s%s", r.Host, r.URL.String())

	req, err := http.NewRequest(r.Method, url_string, r.Body)
	if err != nil {
		log.Printf("Request creation failed for Host <%s>: %s\n", r.Host, err)
		internalServerError(w)
		return
	}

	host, err := lookup(r.Host)
	if err != nil {
		log.Printf("Lookup failed for Host <%s>: %s\n", r.Host, err)
		internalServerError(w)
		return
	}

	req.URL.Host = host

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Request to <%s> failed: %s\n", host, err)
		internalServerError(w)
		return
	}

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	io.Copy(w, resp.Body)
	resp.Body.Close()
}

func getNameserver () string {
	return nameservers[rand.Intn(len(nameservers))]
}

func main() {
	// XXX flags: useRedis true/false, redisAddr, redisPort
	// XXX flags: nameservers=wurst.brot.de,koffer.geloet.com

	flag.StringVar(&listenAddr, "listenAddr", "127.0.0.1", "interface to listen on")
	flag.IntVar(&listenPort, "listenPort", 8080, "port to listen on")
	flag.BoolVar(&useRedis, "useRedis", false, "wether or not to use a redis cache for DNS results")
	flag.StringVar(&redisHost, "redisHost", "127.0.0.1", "redis host to connect to")
	flag.IntVar(&redisPort, "redisPort", 6379, "redis port to connect to")
	strNameservers := flag.String("nameservers", "8.8.8.8;8.8.4.4", "nameservers to cycle through")

	flag.Parse()

	nameservers = strings.Split(*strNameservers, ";")
	rand.Seed(time.Now().UTC().UnixNano())

	listen := fmt.Sprintf("%s:%d", listenAddr, listenPort)

	dnsHandler := http.HandlerFunc(dnsHandlerFunc)
	log.Printf("starting up on %s.\n", listen)
	log.Fatal(http.ListenAndServe(listen, dnsHandler))
}
