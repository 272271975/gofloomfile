package bloomtests

import (
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func RandTest(t *testing.T, filter BloomFilter, n int) {
	for i := 0; i < n; i++ {
		filter.PutString(fmt.Sprintf("r%d", i))
	}

	var miss int

	for i := 0; i < n; i++ {
		exists_record := fmt.Sprintf("r%d", i)
		not_exists_record := fmt.Sprintf("rr%d", i)
		if !filter.HasString(exists_record) {
			miss++
		}

		if filter.HasString(not_exists_record) {
			miss++
		}
	}
	hit_rate := float64(n-miss) / float64(n)
	fmt.Printf("hit_rate: %f\n", hit_rate)

	if hit_rate < 0.9 {
		t.Fatalf("hit_rate is %f, too low 草", hit_rate)
	}
}

func TestMemoryBloomFilter(t *testing.T) {
	var filter BloomFilter = NewMemoryBloomFilter(64<<20, 5)
	RandTest(t, filter, 50000)

}

func TestFileBloomFilter(t *testing.T) {
	target := "bl.gob"
	defer func() {
		fmt.Println(target)
		// os.Remove(target)
	}()
	var filter BloomFilter = NewFileBloomFilter(target, 64<<20, 5)
	RandTest(t, filter, 50000)
	filter.PutString("test hello")
	filter.Store()
	fmt.Println(filter)
}

func TestReloadFileBloomFilter(t *testing.T) {
	target := "bl.gob"
	var filter BloomFilter = ReloadFileBloomFilter(target)
	fmt.Println(filter)
	fmt.Println(filter.HasString("test hello"))
}

func TestRedisBloomFilter(t *testing.T) {
	cli, err := redis.DialURL("redis://10.1.10.4")
	if err != nil {
		t.Fatal(err)
	}
	var filter BloomFilter = NewRedisBloomFilter(cli, 2000, 5)
	RandTest(t, filter, 50)
}
