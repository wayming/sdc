package cache

import (
	"errors"
	"os"

	"github.com/go-redis/redis"
	"github.com/wayming/sdc/sdclogger"
)

const REDIS_KEY_PROXIES = "proxies"

type CacheManager struct {
	clientHandle *redis.Client
}

func (m *CacheManager) Connect() error {
	os.Setenv("REDISHOST", "redis")
	redisAddr := os.Getenv("REDISHOST") + ":" + os.Getenv("REDISPORT")
	m.clientHandle = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0})

	res, err := m.clientHandle.Ping().Result()
	if err != nil {
		return errors.New("Failed to connect to " + redisAddr + ". Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Connected to %s: %s", redisAddr, res)
	return nil
}

func (m *CacheManager) SetProxy(proxy string) error {
	err := m.clientHandle.SAdd(REDIS_KEY_PROXIES, proxy).Err()
	if err != nil {
		return errors.New("Failed to add " + proxy + " to cache. Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Add %s to cache", proxy)
	return nil
}

func (m *CacheManager) GetProxy() (string, error) {
	proxy, err := m.clientHandle.SRandMember(REDIS_KEY_PROXIES).Result()
	if err != nil {
		return "", errors.New("Failed to get a proxy from cache. Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Get %s from cache", proxy)
	return proxy, nil
}

func (m *CacheManager) DeleteProxy(proxy string) error {
	_, err := m.clientHandle.SRem(REDIS_KEY_PROXIES, proxy).Result()
	if err != nil {
		return errors.New("Failed to remove " + proxy + " from cache. Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Remove %s from cache", proxy)
	return nil
}
