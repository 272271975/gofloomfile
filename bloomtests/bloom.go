package bloomtests

/**
假设有两个数，A和B。B为2^n，期中n>=0，A>=0。则：
要求A * B的话，则可使用<<操作符，A << n。
要求A / B的话，则可使用>>操作符，A >> n。
要求A % B的话，则可使用&操作符，A&(B-1)。
**/
import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"os"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
)

type User struct {
	Id   int
	Name string
}

//过滤器 接口定义
type BloomFilter interface {
	Put([]byte)
	PutString(string)
	Has([]byte) bool
	HasString(string) bool
	Close()
	Store()
}

type FileBloomFilter struct {
	*MemoryBloomFilter
	Filename string
}

type MemoryBloomFilter struct {
	K  uint
	Bs BitSets
}

type RedisBloomFilter struct {
	cli redis.Conn
	N   uint
	K   uint
}

func HashData(data []byte, seed uint) uint {
	sha_data := sha256.Sum256(data)
	data = sha_data[:]
	m := murmur3.New64WithSeed(uint32(seed))
	m.Write(data)
	return uint(m.Sum64())
}

// NewMemoryBloomFilter 创建一个内存的bloom filter
func NewMemoryBloomFilter(n uint, k uint) *MemoryBloomFilter {
	return &MemoryBloomFilter{
		K:  k,
		Bs: NewBitSets(n),
	}
}

func (filter *MemoryBloomFilter) Put(data []byte) {
	len := uint(len(filter.Bs))
	for i := uint(0); i < filter.K; i++ {
		filter.Bs.Set(HashData(data, i) % len)
	}
}

func (filter *MemoryBloomFilter) PutString(data string) {
	filter.Put([]byte(data))
}

// Has 推测记录是否已存在
func (filter *MemoryBloomFilter) Has(data []byte) bool {
	len := uint(len(filter.Bs))

	for i := uint(0); i < filter.K; i++ {
		if !filter.Bs.IsSet(HashData(data, i) % len) {
			return false
		}
	}

	return true
}

// Has 推测记录是否已存在
func (filter *MemoryBloomFilter) HasString(search string) bool {
	return filter.Has(Str2byte(search))
}

// Close 关闭bloom filter
func (filter *MemoryBloomFilter) Close() {
	filter.Bs = nil
}

// NewFileBloomFilter 创建一个以文件为存储介质的bloom filter
// target 文件保存处
// 本质上就是增加了MemoryBloomFilter, 在创建时打开文件, 在Close时保存文件
func NewFileBloomFilter(filename string, n uint, k uint) *FileBloomFilter {
	memory_filter := NewMemoryBloomFilter(n, k)
	filter := &FileBloomFilter{
		memory_filter, filename,
	}
	return filter
}

//从文件中 加载已经创建的过滤器 bitsets
func ReloadFileBloomFilter(filename string) *FileBloomFilter {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Open file"))
	}
	defer f.Close()
	gzip_reader, _ := gzip.NewReader(f)
	defer gzip_reader.Close()

	var filter FileBloomFilter
	decoder := gob.NewDecoder(gzip_reader)
	decoder.Decode(&filter)
	return &filter
}

func (filter *FileBloomFilter) Close() {
	filter.Store()
	filter.Bs = nil
}

//使用zip 压缩 大概节省90% 空间 压缩效果很明显
func (filter *FileBloomFilter) Store() {
	f, err := os.Create(filter.Filename)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Open file"))
	}
	defer f.Close()

	gzip_writer := gzip.NewWriter(f)
	defer gzip_writer.Close()

	encoder := gob.NewEncoder(gzip_writer)
	err = encoder.Encode(filter)
	fmt.Println(filter)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "gzip"))
	}
}

func (filter *FileBloomFilter) reStore() {
	f, err := os.Open(filter.Filename)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatalf("%+v", errors.Wrap(err, "Open file"))
	}
	defer f.Close()

	gzip_reader, err := gzip.NewReader(f)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Ungzip"))
	}

	decoder := gob.NewDecoder(gzip_reader)
	err = decoder.Decode(filter)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "gob decode"))
	}
}

func NewRedisBloomFilter(cli redis.Conn, n, k uint) *RedisBloomFilter {
	filter := &RedisBloomFilter{
		cli: cli,
		N:   n,
		K:   k,
	}
	length, _ := redis.Int64(cli.Do("LLEN", filter.redisKey()))
	if uint(length) != n {
		bs := make([]interface{}, n)
		push_args := []interface{}{filter.redisKey()}
		push_args = append(push_args, bs...)
		cli.Do("DEL", filter.redisKey())
		cli.Do("LPUSH", push_args...)
	}

	return filter
}

func (filter *RedisBloomFilter) Put(data []byte) {
	for i := uint(0); i < filter.K; i++ {
		_, err := filter.cli.Do("LSET", filter.redisKey(), HashData(data, i)%filter.N, "1")
		if err != nil {
			log.Fatalf("%+v", errors.Wrap(err, "LSET"))
		}
	}
}

func (filter *RedisBloomFilter) PutString(data string) {
	filter.Put([]byte(data))
}

func (filter *RedisBloomFilter) Has(data []byte) bool {
	for i := uint(0); i < filter.K; i++ {
		index := HashData(data, i) % filter.N
		value, err := redis.String(filter.cli.Do("LINDEX", filter.redisKey(), index))
		if err != nil {
			log.Fatalf("%+v", errors.Wrap(err, "LINDEX"))
		}
		if value != "1" {
			return false
		}
	}

	return true
}
func (filter *RedisBloomFilter) HasString(search string) bool {
	return filter.Has(Str2byte(search))
}

// Close 只将cli设置为nil, 关闭redis连接的操作放在调用处
func (filter *MemoryBloomFilter) Store() {
}

// Close 只将cli设置为nil, 关闭redis连接的操作放在调用处
func (filter *RedisBloomFilter) Close() {
	filter.cli = nil
}

// Close 只将cli设置为nil, 关闭redis连接的操作放在调用处
func (filter *RedisBloomFilter) Store() {
	filter.cli = nil
}

// redisKey 根据filter的n和k来生成一个独立的redis key
func (filter *RedisBloomFilter) redisKey() string {
	return fmt.Sprintf("_bloomfilter:n%d:k%d", filter.N, filter.K)
}
