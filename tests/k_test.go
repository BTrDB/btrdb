package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/coreos/etcd/clientv3"
	"github.com/pborman/uuid"
)

func BenchmarkWriteOMAPTypical(b *testing.B) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()
	ioctx, err := conn.OpenIOContext("btrdb")
	if err != nil {
		panic(err)
	}

	st1 := uuid.NewRandom().String()
	st2 := uuid.NewRandom().String()
	st3 := uuid.NewRandom().String()
	st4 := uuid.NewRandom().String()
	{
		k := uuid.NewRandom().String()
		v := uuid.NewRandom()[:]
		kv := map[string][]byte{k: v}
		ioctx.SetOmap(st1, kv)
		ioctx.SetOmap(st2, kv)
		ioctx.SetOmap(st3, kv)
		ioctx.SetOmap(st4, kv)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := uuid.NewRandom().String()
		v := uuid.NewRandom()[:]
		kv := map[string][]byte{k: v}
		ioctx.SetOmap(st1, kv)
		ioctx.SetOmap(st2, kv)
		ioctx.SetOmap(st3, kv)
		ioctx.SetOmap(st4, kv)
	}
}

func BenchmarkReadOMAPTypical(b *testing.B) {
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	conn.Connect()
	ioctx, err := conn.OpenIOContext("btrdb")
	if err != nil {
		panic(err)
	}

	st1 := uuid.NewRandom().String()
	st2 := uuid.NewRandom().String()
	st3 := uuid.NewRandom().String()
	st4 := uuid.NewRandom().String()

	k := uuid.NewRandom().String()
	v := uuid.NewRandom()[:]
	kv := map[string][]byte{k: v}
	ioctx.SetOmap(st1, kv)
	ioctx.SetOmap(st2, kv)
	ioctx.SetOmap(st3, kv)
	ioctx.SetOmap(st4, kv)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ioctx.GetOmapValues(st1, "", "", 100)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkEtcdWriteTypical(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Fatalf("etcd error %v", err)
	}

	st1 := uuid.NewRandom().String()
	st2 := uuid.NewRandom().String()
	st3 := uuid.NewRandom().String()
	st4 := uuid.NewRandom().String()
	{
		k := uuid.NewRandom().String()
		v := uuid.NewRandom().String()

		cli.Put(context.Background(), st1+"/"+k, v)
		cli.Put(context.Background(), st2+"/"+k, v)
		cli.Put(context.Background(), st3+"/"+k, v)
		cli.Put(context.Background(), st4+"/"+k, v)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := uuid.NewRandom().String()
		v := uuid.NewRandom().String()

		cli.Put(context.Background(), st1+"/"+k, v)
		cli.Put(context.Background(), st2+"/"+k, v)
		cli.Put(context.Background(), st3+"/"+k, v)
		cli.Put(context.Background(), st4+"/"+k, v)
	}
}

func BenchmarkEtcdReadTypical(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Fatalf("etcd error %v", err)
	}

	st1 := uuid.NewRandom().String()
	st2 := uuid.NewRandom().String()
	st3 := uuid.NewRandom().String()
	st4 := uuid.NewRandom().String()

	k := uuid.NewRandom().String()
	v := uuid.NewRandom().String()

	cli.Put(context.Background(), st1+"/"+k, v)
	cli.Put(context.Background(), st2+"/"+k, v)
	cli.Put(context.Background(), st3+"/"+k, v)
	cli.Put(context.Background(), st4+"/"+k, v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cli.Get(context.Background(), st1+"/"+k)
		cli.Get(context.Background(), st2+"/"+k)
		cli.Get(context.Background(), st3+"/"+k)
		cli.Get(context.Background(), st4+"/"+k)
	}
}

func BenchmarkEtcdReadTypicalPrefix(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Fatalf("etcd error %v", err)
	}

	st1 := uuid.NewRandom().String()
	st2 := uuid.NewRandom().String()
	st3 := uuid.NewRandom().String()
	st4 := uuid.NewRandom().String()

	k := uuid.NewRandom().String()
	v := uuid.NewRandom().String()

	cli.Put(context.Background(), st1+"/"+k, v)
	cli.Put(context.Background(), st2+"/"+k, v)
	cli.Put(context.Background(), st3+"/"+k, v)
	cli.Put(context.Background(), st4+"/"+k, v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cli.Get(context.Background(), st1+"/", clientv3.WithPrefix())
		cli.Get(context.Background(), st2+"/", clientv3.WithPrefix())
		cli.Get(context.Background(), st3+"/", clientv3.WithPrefix())
		cli.Get(context.Background(), st4+"/", clientv3.WithPrefix())
	}
}

func BenchmarkEtcdReadTypicalSerializable(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Fatalf("etcd error %v", err)
	}

	st1 := uuid.NewRandom().String()
	st2 := uuid.NewRandom().String()
	st3 := uuid.NewRandom().String()
	st4 := uuid.NewRandom().String()

	k := uuid.NewRandom().String()
	v := uuid.NewRandom().String()

	cli.Put(context.Background(), st1+"/"+k, v)
	cli.Put(context.Background(), st2+"/"+k, v)
	cli.Put(context.Background(), st3+"/"+k, v)
	cli.Put(context.Background(), st4+"/"+k, v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cli.Get(context.Background(), st1+"/"+k, clientv3.WithSerializable())
		cli.Get(context.Background(), st2+"/"+k, clientv3.WithSerializable())
		cli.Get(context.Background(), st3+"/"+k, clientv3.WithSerializable())
		cli.Get(context.Background(), st4+"/"+k, clientv3.WithSerializable())
	}
}

func BenchmarkEtcdReadTypicalPrefixSerializable(b *testing.B) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		b.Fatalf("etcd error %v", err)
	}

	st1 := uuid.NewRandom().String()
	st2 := uuid.NewRandom().String()
	st3 := uuid.NewRandom().String()
	st4 := uuid.NewRandom().String()

	k := uuid.NewRandom().String()
	v := uuid.NewRandom().String()

	cli.Put(context.Background(), st1+"/"+k, v)
	cli.Put(context.Background(), st2+"/"+k, v)
	cli.Put(context.Background(), st3+"/"+k, v)
	cli.Put(context.Background(), st4+"/"+k, v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cli.Get(context.Background(), st1+"/", clientv3.WithPrefix(), clientv3.WithSerializable())
		cli.Get(context.Background(), st2+"/", clientv3.WithPrefix(), clientv3.WithSerializable())
		cli.Get(context.Background(), st3+"/", clientv3.WithPrefix(), clientv3.WithSerializable())
		cli.Get(context.Background(), st4+"/", clientv3.WithPrefix(), clientv3.WithSerializable())
	}
}
