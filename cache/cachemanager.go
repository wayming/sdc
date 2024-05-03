package cache

import (
	"errors"
	"os"

	"github.com/go-redis/redis"
	"github.com/wayming/sdc/sdclogger"
)

type CacheManager struct {
	clientHandle *redis.Client
}

func NewCacheManager() *CacheManager {
	return &CacheManager{clientHandle: nil}
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

func (m *CacheManager) AddToSet(key string, value string) error {
	err := m.clientHandle.SAdd(key, value).Err()
	if err != nil {
		return errors.New("Failed to add " + value + " to cache key " + key + ". Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Add %s to cache key %s", value, key)
	return nil
}

func (m *CacheManager) GetFromSet(key string) (string, error) {
	length, err := m.GetLength(key)
	if err != nil {
		return "", errors.New("Failed to get the length of set key " + key + " from cache. Error: " + err.Error())
	}
	if length == 0 {
		return "", nil
	}

	value, err := m.clientHandle.SRandMember(key).Result()
	if err != nil {
		return "", errors.New("Failed to get a value from cache key " + key + ". Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Get %s from cache key %s", value, key)
	return value, nil
}

func (m *CacheManager) GetAllFromSet(key string) ([]string, error) {
	allMembers, err := m.clientHandle.SMembers(key).Result()
	if err != nil {
		return nil, errors.New("Failed to get the all members of set key " + key + " from cache. Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Get %d members from cache key %s", len(allMembers), key)
	return allMembers, nil
}

func (m *CacheManager) DeleteFromSet(key string, value string) error {
	_, err := m.clientHandle.SRem(key, value).Result()
	if err != nil {
		return errors.New("Failed to remove " + value + " from cache key " + key + ". Error: " + err.Error())
	}
	sdclogger.SDCLoggerInstance.Printf("Remove %s from cache key %s", value, key)
	return nil
}

func (m *CacheManager) GetLength(key string) (int64, error) {
	length, err := m.clientHandle.SCard(key).Result()
	if err != nil {
		return 0, errors.New("Failed to get the length of set key " + key + " from cache. Error: " + err.Error())
	}
	return length, nil
}
