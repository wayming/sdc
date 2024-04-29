package cache

import (
	"testing"

	"github.com/go-redis/redis"
)

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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &CacheManager{
				clientHandle: tt.fields.clientHandle,
			}
			if err := m.Connect(); (err != nil) != tt.wantErr {
				t.Errorf("CacheManager.Connect() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
