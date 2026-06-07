// Package cache is a thin cache-aside layer over Redis. Callers own the key,
// TTL, and a loader that reads from the system of record (e.g. Postgres) on a
// miss; the layer handles the lookup, populate-on-miss, and invalidation
// mechanics. Keeping it value-agnostic means it is not tied to any one domain.
package cache

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Cache {
	return &Cache{rdb: rdb}
}

// GetInt returns the integer cached at key. On a miss it calls load; if that
// reports found, the value is written back with ttl. found=false (e.g. an
// unknown entity) is propagated to the caller and deliberately not cached. A
// failed populate is best-effort — it only costs a future miss.
func (c *Cache) GetInt(ctx context.Context, key string, ttl time.Duration, load func(context.Context) (int, bool, error)) (int, bool, error) {
	v, err := c.rdb.Get(ctx, key).Int()
	if err == nil {
		return v, true, nil
	}
	if err != redis.Nil {
		return 0, false, err
	}

	val, found, err := load(ctx)
	if err != nil || !found {
		return 0, found, err
	}

	if err := c.rdb.Set(ctx, key, val, ttl).Err(); err != nil {
		slog.Warn("cache: populate failed", "key", key, "err", err)
	}
	return val, true, nil
}

// Invalidate removes key so the next read reloads from the source. Call after
// persisting a change to the underlying value. A bounded TTL on the cached
// entry is the backstop for any invalidation that is lost.
func (c *Cache) Invalidate(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, key).Err()
}
