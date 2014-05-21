package main

import (
	"fmt"
	"log"
	"net/http"
	"io"
	"errors"
	"flag"
	"github.com/miekg/dns"
	"github.com/garyburd/redigo/redis"
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
	msg := new(dns.Msg)
	msg.SetQuestion(fmt.Sprintf("%s.", host), dns.TypeA)

	c := new(dns.Client)
	in, _, err := c.Exchange(msg, "ns1.jimdo.com:53")
	if err != nil {
		return "", 0, err
	}

	if t, ok := in.Answer[0].(*dns.A); ok {
		return t.A.String(), t.Header().Ttl, nil
	}

	if t, ok := in.Answer[0].(*dns.CNAME); ok {
		return t.Target, t.Header().Ttl, nil
	}

	return "", 0, errors.New("lookup failed.")
}

func lookup (host string) (string, error) {

	redis_conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Fatal("cannot connect to redis: ", err)
	}
	defer redis_conn.Close()

	h, hit, err := redisGet(redis_conn, host)
	if err != nil {
		log.Println("redisGet error: ", err)
	} else if hit {
		log.Printf("Redis hit for Host <%s>: <%s>\n", host, h)
		return h, nil
	}

	h, ttl, err := dnsLookup(host)
	if err != nil {
		return "", err
	}

	if err := redisSet(redis_conn, host, h, ttl); err != nil {
		log.Println("redisSet error: ", err)
	}

	log.Printf("DNS hit for Host <%s>: <%s>\n", host, h)
	return h, nil
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

func main() {
	// XXX flags: useRedis true/false, redisAddr, redisPort
	// XXX flags: nameservers=wurst.brot.de,koffer.geloet.com

	listenAddr := flag.String("listenAddr", "127.0.0.1", "interface to listen on")
	listenPort := flag.Int("listenPort", 8080, "port to listen on")

	flag.Parse()

	listen := fmt.Sprintf("%s:%d", *listenAddr, *listenPort)

	dnsHandler := http.HandlerFunc(dnsHandlerFunc)
	log.Printf("starting up on %s.\n", listen)
	log.Fatal(http.ListenAndServe(listen, dnsHandler))
}
