package cache

import (
	"errors"
	"os"

	"github.com/go-redis/redis"
	"github.com/wayming/sdc/sdclogger"
)

type CacheManager struct {
	ProxyCacheKey string
	clientHandle  *redis.Client
}

func NewCacheManager() *CacheManager {
	return &CacheManager{ProxyCacheKey: "PROXIES", clientHandle: nil}
}

func (m *CacheManager) SetProxyKey(key string) {
	m.ProxyCacheKey = key
}

func (m *CacheManager) Disconnect() error {
	if m.clientHandle == nil {
		sdclogger.SDCLoggerInstance.Printf("No redis connection to close")
		return nil
	}

	if err := m.clientHandle.Close(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Faield to disconnect from redis. Error %s", err.Error())
	}
	m.clientHandle = nil

	return nil
}

func (m *CacheManager) Connect() error {
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
	err := m.clientHandle.SAdd(m.ProxyCacheKey, proxy).Err()
	if err != nil {
		return errors.New("Failed to add " + proxy + " to cache. Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Add %s to cache", proxy)
	return nil
}

func (m *CacheManager) GetProxy() (string, error) {
	length, err := m.Proxies()
	if err != nil {
		return "", errors.New("Failed to get the length of proxy set from cache. Error: " + err.Error())
	}
	if length == 0 {
		return "", nil
	}

	proxy, err := m.clientHandle.SRandMember(m.ProxyCacheKey).Result()
	if err != nil {
		return "", errors.New("Failed to get a proxy from cache. Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Get %s from cache", proxy)
	return proxy, nil
}

func (m *CacheManager) DeleteProxy(proxy string) error {
	_, err := m.clientHandle.SRem(m.ProxyCacheKey, proxy).Result()
	if err != nil {
		return errors.New("Failed to remove " + proxy + " from cache. Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Remove %s from cache", proxy)
	return nil
}

func (m *CacheManager) Proxies() (int64, error) {
	length, err := m.clientHandle.SCard(m.ProxyCacheKey).Result()
	if err != nil {
		return 0, errors.New("Failed to get the length of proxy set from cache. Error: " + err.Error())
	}
	return length, nil
}
