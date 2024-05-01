package cache

import (
	"os"
	"testing"

	"github.com/go-redis/redis"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

func SetupCacheManagerTest(testName string) {
	testcommon.SetupTest(testName)
}

func TeardownCacheManagerTest() {
	redisAddr := os.Getenv("REDISHOST") + ":" + os.Getenv("REDISPORT")
	redisHandle := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0})
	defer redisHandle.Close()

	if err := redisHandle.Del(REDIS_KEY_PROXIES).Err(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Faield to drop cache set %s. Error: %s", REDIS_KEY_PROXIES, err.Error())
	}

	testcommon.TeardownTest()
}

func TestCacheManager_Connect(t *testing.T) {
	type fields struct {
		clientHandle *redis.Client
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "TestCacheManager_Connect",
			fields: fields{
				clientHandle: nil,
			},
			wantErr: false,
		},
	}

	SetupCacheManagerTest(t.Name())
	defer TeardownCacheManagerTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewCacheManager()
			if err := m.Connect(); (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.Connect() error = %v, wantErr %v", err, tt.wantErr)
			}

			if m.clientHandle == nil {
				t.Errorf("Faild to establish redis connection")
			}

			if err := m.Disconnect(); (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.Disconnect() error = %v, wantErr %v", err, tt.wantErr)
			}

			if m.clientHandle != nil {
				t.Errorf("Faild to drop redis connection")
			}
		})
	}
}

func TestCacheManager_ProxyCache(t *testing.T) {
	type fields struct {
		clientHandle *redis.Client
	}
	type args struct {
		proxy string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "TestCacheManager_Connect",
			fields: fields{
				clientHandle: nil,
			},
			args: args{
				"1.1.1.1:8080",
			},
			wantErr: false,
		},
	}

	SetupCacheManagerTest(t.Name())
	defer TeardownCacheManagerTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &CacheManager{
				clientHandle: tt.fields.clientHandle,
			}

			if err := m.Connect(); (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.Connect() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err := m.SetProxy(tt.args.proxy); (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.SetProxy() error = %v, wantErr %v", err, tt.wantErr)
			}

			proxy, err := m.GetProxy()
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.GetProxy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if proxy != tt.args.proxy {
				t.Errorf("CacheManager.GetProxy() expecting %s, got %s", tt.args.proxy, proxy)
			}

			if err := m.DeleteProxy(tt.args.proxy); (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.DeleteProxy() error = %v, wantErr %v", err, tt.wantErr)
			}
			proxy, err = m.GetProxy()
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.GetProxy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if proxy != "" {
				t.Errorf("CacheManager.GetProxy() expecting %s, got %s", "", proxy)
			}

		})
	}
}
