# Search client traits

rustac provides a family of traits for querying STAC APIs and local datasets.
Each trait covers a different access pattern; adapter structs let you convert between them without writing boilerplate.

## Trait overview

| Trait | What it does | Required method |
| ----- | ------------ | --------------- |
| [`ItemsClient`](https://docs.rs/stac/latest/stac/api/trait.ItemsClient.html) | Fetch one page of items | `search` |
| [`StreamItemsClient`](https://docs.rs/stac/latest/stac/api/trait.StreamItemsClient.html) | Stream all items across pages | `search_stream` |
| [`CollectionsClient`](https://docs.rs/stac/latest/stac/api/trait.CollectionsClient.html) | Fetch all collections at once | `collections` |
| [`PagedCollectionsClient`](https://docs.rs/stac/latest/stac/api/trait.PagedCollectionsClient.html) | Fetch one page of collections with cursor | `collections_page` |
| [`StreamCollectionsClient`](https://docs.rs/stac/latest/stac/api/trait.StreamCollectionsClient.html) | Stream all collections | `collections_stream` |
| [`ArrowItemsClient`](https://docs.rs/stac/latest/stac/api/trait.ArrowItemsClient.html) *(geoarrow feature)* | Return items as Arrow record batches | `search_to_arrow` |
| [`TransactionClient`](https://docs.rs/stac/latest/stac/api/trait.TransactionClient.html) | Write items and collections | `add_item`, `add_collection` |

## Why `Stream` instead of `Iterator`?

`std::iter::Iterator::next` is a synchronous call.
Driving pagination against a remote API or a DuckDB file on each call would block the async runtime.
`futures::stream::Stream` is the async equivalent of `Iterator` — `StreamExt::next().await` is precisely the async `next()`.
The `StreamExt` combinator library (`map`, `filter`, `take`, `collect`, …) covers all the same operations you would use on a sync iterator.

If you need a synchronous/blocking iterator (e.g. for a CLI pipeline), wrap a tokio runtime:

```rust
let rt = tokio::runtime::Handle::current();
rt.block_on(async {
    let items = client.collect_items(Search::default()).await?;
    // …
    Ok::<_, Error>(())
})
```

## Default convenience methods

Both streaming traits come with default implementations so you usually don't need to touch the stream directly:

```rust
// Collect all items across all pages
let items: Vec<Item> = client.collect_items(Search::default()).await?;

// Count without keeping items in memory
let count: usize = client.item_count(Search::default()).await?;

// Collect all collections
let cols: Vec<Collection> = client.collect_collections().await?;
```

## Adapters

The `stac::api` module exports adapter structs that convert between traits.

### `PagedItemsStream` — `ItemsClient` → `StreamItemsClient`

```rust
use stac::api::{PagedItemsStream, Search, StreamItemsClient};

// Any ItemsClient becomes a StreamItemsClient.
let streaming = PagedItemsStream::new(my_items_client);
let items = streaming.collect_items(Search::default()).await?;
```

Pagination works via the `ItemCollection::next` field (token/skip-based backends
such as pgstac and the in-memory backend).
For link-based HTTP pagination (a remote STAC API), use `stac-io`'s native
`StreamItemsClient` implementation which follows `rel="next"` links.

### `CollectionsClient` — automatic `StreamCollectionsClient`

Any `CollectionsClient + Clone + Send + Sync` automatically implements
`StreamCollectionsClient` via a blanket — no wrapper needed:

```rust
// Any CollectionsClient is already a StreamCollectionsClient.
let cols = my_collections_client.collect_collections().await?;
```

Collections are fetched eagerly in a single call because the current STAC API
spec does not define a paginated collections endpoint for most backends.

### `PagedCollectionsClient` — cursor-paginated collections

For backends that support cursor-paginated `/collections` (e.g. a future pgstac
version), implement `PagedCollectionsClient` instead of `CollectionsClient`.
Then implement `StreamCollectionsClient` using `stream_pages_collections_generic`:

```rust
use stac::api::{
    PagedCollectionsClient, StreamCollectionsClient, stream_pages_collections_generic,
};
use stac::Collection;

impl StreamCollectionsClient for MyPagedBackend {
    type Error = MyError;

    async fn collections_stream(
        &self,
    ) -> Result<
        impl futures_core::Stream<Item = Result<Collection, MyError>> + Send,
        MyError,
    > {
        let (first_page, first_token) = self.collections_page(None).await?;
        Ok(stream_pages_collections_generic(self.clone(), first_page, first_token))
    }
}
```

This is exactly parallel to how `stream_pages_generic` is used for items.

### `ArrowItemsClient` *(geoarrow feature)* — automatic `ItemsClient` + `StreamItemsClient`

Any type that implements `ArrowItemsClient + Send + Sync` automatically satisfies
both `ItemsClient` and `StreamItemsClient` via blanket implementations — no wrapper
struct required:

```rust
// HrefClient implements ArrowItemsClient.
// If it were also Sync, these would just work:
let page = my_arrow_client.search(Search::default()).await?;
let items = my_arrow_client.collect_items(Search::default()).await?;
```

The blanket impl eagerly collects all record batches synchronously (while holding
the `&self` borrow required by `search_to_arrow`) and returns owned data.

!!! note "DuckDB and `!Sync`"
    `duckdb::Connection` is `!Sync`, so `HrefClient` doesn't satisfy the `+ Sync`
    bound and doesn't get the blanket impl.  `SyncHrefClient` solves this with a
    `Mutex`, but `Mutex<T>` can't implement `ArrowItemsClient` because the
    reader returned by `search_to_arrow` must borrow from `&self` (it can't
    outlive the lock guard).  `SyncHrefClient` therefore has explicit
    `ItemsClient` and `StreamItemsClient` impls that lock/unlock around each
    synchronous query.

## Adapter conversion chart

```text
ItemsClient ──────────── PagedItemsStream ──────────► StreamItemsClient
                                                               ▲
ArrowItemsClient + Sync ─────── (blanket) ────────► ItemsClient
                        └─────── (blanket) ────────► StreamItemsClient

CollectionsClient + Clone + Sync ────── (blanket) ──► StreamCollectionsClient
                                                               ▲
PagedCollectionsClient ── stream_pages_collections_generic ────┘
```

## Which adapter should I use?

| I have... | I want... | How |
| --- | --- | --- |
| `ItemsClient` | streaming / `collect_items` | `PagedItemsStream` |
| `ArrowItemsClient + Sync` | `ItemsClient` or `StreamItemsClient` | automatic (blanket impl) |
| `CollectionsClient + Clone + Sync` | `StreamCollectionsClient` | automatic (blanket impl) |
| `PagedCollectionsClient` | `StreamCollectionsClient` | call `stream_pages_collections_generic` in your impl |

## Pagination mechanics

Token/skip backends (pgstac, in-memory, DuckDB) signal the next page via
`ItemCollection::next` — a free-form `serde_json::Map` whose entries are merged
into `Search::additional_fields` for the subsequent request.

The `stream_pages_generic` free function (also exported from `stac::api`)
drives this loop and is used internally by `PagedItemsStream` and all
`stac-server` backends.  You can call it directly if you need a stream starting
from an already-fetched first page:

```rust
use stac::api::{Search, ItemsClient, stream_pages_generic};

let search = Search::default();
let first_page = client.search(search.clone()).await?;
let stream = stream_pages_generic(client, search, first_page);
```

The `stream_pages_collections_generic` free function is the collections
equivalent, for use with `PagedCollectionsClient` backends:

```rust
use stac::api::{PagedCollectionsClient, stream_pages_collections_generic};

let (first_page, first_token) = client.collections_page(None).await?;
let stream = stream_pages_collections_generic(client, first_page, first_token);
```

## Performance notes

- **Memory**: prefer `search_stream` + `StreamExt` combinators over
  `collect_items` when result sets are large.
- **Clone cost**: `stream_pages_generic` clones the `Search` value on each page
  to merge the new pagination token.  For searches with large filter expressions
  this can be reduced by pre-computing a compact `Search` baseline and keeping
  filter data outside `additional_fields`.
- **Arrow**: `ArrowItemsClient` types that are `Send + Sync` get `ItemsClient` and
  `StreamItemsClient` for free via blanket impls.  Record batch decoding is eager
  (all batches collected synchronously).  For zero-copy Arrow processing, implement
  `ArrowItemsClient` directly and call `search_to_arrow` synchronously in a
  `spawn_blocking` task.
