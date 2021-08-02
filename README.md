## Cache files with Cloudflare kv / LRU cache

```rust
    let cache_handler = CfKvFs::builder("https://example.com", "path")
        .auth("Bearer 12345")
        .table("test1")
        .build()
        .unwrap();
    let data = cache_handler.get_blob("test.bin").unwrap();
```
