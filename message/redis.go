package message

import (
	"fmt"
	"time"

	"log"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	configure "github.com/enriqueChen/mola/config"
)

// RedisConn ...
type RedisConn struct {
	Conf   *configure.RedisConf
	Logger *logrus.Entry
	c      *redis.Client
}

const (
	redisRetry   int = 3
	connPoolSize int = 5
)

// NewRedisConn create a redis conn client with pool
func NewRedisConn(conf *configure.RedisConf, logger *logrus.Entry) (ret *RedisConn, err error) {
	ret = &RedisConn{
		Conf:   conf,
		Logger: logger,
	}
	_, err = ret.NewClient()
	return
}

// NewClient Get New Redis connection client
func (r *RedisConn) NewClient() (client *redis.Client, err error) {
	client = redis.NewClient(&redis.Options{
		Addr:       r.Conf.Addr,
		Password:   r.Conf.Password,
		DB:         r.Conf.DB,
		MaxRetries: redisRetry,
		PoolSize:   connPoolSize,
	})
	redis.SetLogger(log.New(r.Logger.Logger.Out, "redis: ", log.LstdFlags|log.Lshortfile))
	err = client.Ping().Err()
	r.c = client
	return
}

// Set set the value of the key
func (r *RedisConn) Set(key string, value interface{}, expire time.Duration) (err error) {
	return r.c.Set(key, value, expire).Err()
}

// Get get the key value
func (r *RedisConn) Get(key string) (res interface{}, err error) {
	return r.c.Get(key).Result()
}

// Keys get the key value
func (r *RedisConn) Keys(pattern string) (res []string, err error) {
	res, err = r.c.Keys(pattern).Result()
	return
}

// Del get the key value
func (r *RedisConn) Del(key string) (res bool, err error) {
	var affected int64
	affected, err = r.c.Del(key).Result()
	res = (affected == int64(1))
	return
}

var (
	// LockExpireAfter is a global LockExpireTime configuration
	LockExpireAfter = 6 * time.Second
)

// RedisLock give a redis lock to application
type RedisLock struct {
	key       string
	redisConn *RedisConn
}

// Locker produce a Lock on specified key
func (r *RedisConn) Locker(key string) *RedisLock {
	return &RedisLock{
		key:       fmt.Sprintf("lock:%s", key),
		redisConn: r,
	}
}

// LockWithTimeout try to lock the key with a expiretime, return true if got the lock.
func (l *RedisLock) LockWithTimeout(expireTime time.Duration) (gotLock bool) {
	gotLock, _ = l.redisConn.c.SetNX(l.key, "lock", expireTime).Result()
	return
}

// Unlock the key
func (l *RedisLock) Unlock(key string, expireTime time.Duration) (ret bool) {
	delRet, _ := l.redisConn.c.Del(l.key).Result()
	ret = (delRet != int64(1))
	return
}
