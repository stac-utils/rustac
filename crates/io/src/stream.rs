//! Streaming JSON writer for a search-response [`stac::api::ItemCollection`]: the
//! `features` array is written one item at a time, and the rest of the collection (links, context,
//! counts) is supplied by a `finalize` callback after the items drain (the `next` link needs the last
//! item; a `numberMatched` count may run concurrently).

use futures::{Stream, StreamExt};
use serde_json::Value;
use stac::api::{ItemCollection, Search};
use std::{future::Future, io::Write, pin::Pin};

/// A boxed error so backend (item stream, finalize) and writer (IO/serialization) errors compose.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A boxed, pinned stream of serialized STAC items.
pub type ItemStream = Pin<Box<dyn Stream<Item = Result<Value, BoxError>> + Send>>;

/// Produces the finished [`ItemCollection`] (with empty `items`; the writer fills `numberReturned`) from
/// the first item, the last item, and the number written. Called once, after the stream drains.
pub type Finalize = Box<
    dyn FnOnce(
            Option<Value>,
            Option<Value>,
            u64,
        ) -> Pin<Box<dyn Future<Output = Result<ItemCollection, BoxError>> + Send>>
        + Send,
>;

/// A backend's streamed search: the item stream plus the finalizer for the collection footer.
pub struct StreamedSearch {
    /// The response items, streamed one at a time.
    pub items: ItemStream,
    /// Produces the finished collection once the items drain.
    pub finalize: Finalize,
}

/// A backend that streams a search response as items plus a finished [`ItemCollection`]. How it produces
/// and paginates them is the implementation's concern.
///
/// `context` requests `numberMatched` (the STAC context extension). `self_href` is the URL this response
/// is served at; pagination links are absolute against it, or relative when `None`.
pub trait StreamSearch: Send + Sync {
    /// Begins a streamed search, capped at `max_items` total items.
    fn stream_search(
        &self,
        search: Search,
        max_items: Option<usize>,
        context: bool,
        self_href: Option<String>,
    ) -> impl Future<Output = Result<StreamedSearch, BoxError>> + Send;

    /// Drives this backend's streamed search into `writer` as one flat-memory JSON `ItemCollection`,
    /// returning the number of items written.
    fn write_search<W: Write>(
        &self,
        search: Search,
        max_items: Option<usize>,
        context: bool,
        self_href: Option<String>,
        writer: W,
        pretty: bool,
    ) -> impl Future<Output = Result<u64, BoxError>> {
        async move {
            let StreamedSearch { items, finalize } = self
                .stream_search(search, max_items, context, self_href)
                .await?;
            write_item_collection(writer, items, pretty, finalize).await
        }
    }
}

/// Writes a search response as a streamed `FeatureCollection`: the `features` array is written one item
/// at a time, then `finalize` supplies the footer (links + optional count). The bytes equal
/// `serde_json`-serializing the equivalent [`ItemCollection`], pretty or compact. Returns the item count.
pub async fn write_item_collection<W, S, F, Fut>(
    mut writer: W,
    items: S,
    pretty: bool,
    finalize: F,
) -> Result<u64, BoxError>
where
    W: Write,
    S: Stream<Item = Result<Value, BoxError>>,
    F: FnOnce(Option<Value>, Option<Value>, u64) -> Fut,
    Fut: Future<Output = Result<ItemCollection, BoxError>>,
{
    writer.write_all(if pretty {
        b"{\n  \"type\": \"FeatureCollection\",\n  \"features\": ["
    } else {
        b"{\"type\":\"FeatureCollection\",\"features\":["
    })?;

    futures::pin_mut!(items);
    let mut first: Option<Value> = None;
    let mut pending: Option<Value> = None;
    let mut count: u64 = 0;
    while let Some(item) = items.next().await {
        let item = item?;
        if let Some(previous) = pending.take() {
            write_element(&mut writer, &previous, count, pretty)?;
            count += 1;
        } else {
            first = Some(item.clone());
        }
        pending = Some(item);
    }
    if let Some(last) = &pending {
        write_element(&mut writer, last, count, pretty)?;
        count += 1;
    }
    writer.write_all(if pretty && count > 0 { b"\n  ]" } else { b"]" })?;

    // The footer is the rest of a real ItemCollection (`links`, `numberMatched`, `numberReturned`, …),
    // serialized by serde and spliced in after the streamed features — `type`/`features` dropped since
    // they're already written.
    let mut collection = finalize(first, pending, count).await?;
    collection.number_returned = Some(count);
    let value = serde_json::to_value(&collection)?;
    let members: serde_json::Map<String, Value> = value
        .as_object()
        .expect("an ItemCollection serializes to a JSON object")
        .iter()
        .filter(|(key, _)| key.as_str() != "type" && key.as_str() != "features")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if !members.is_empty() {
        let object = if pretty {
            serde_json::to_string_pretty(&Value::Object(members))?
        } else {
            serde_json::to_string(&Value::Object(members))?
        };
        let inner = object
            .strip_prefix('{')
            .and_then(|rest| rest.strip_suffix('}'))
            .expect("serde_json serializes an object with braces");
        writer.write_all(b",")?;
        writer.write_all(inner.trim_end().as_bytes())?;
    }

    writer.write_all(if pretty { b"\n}" } else { b"}" })?;
    Ok(count)
}

/// Writes one item as an element of the `features` array. `index` is the
/// element's position (0-based); a non-zero index gets a leading separator.
fn write_element<W: Write>(
    writer: &mut W,
    item: &Value,
    index: u64,
    pretty: bool,
) -> Result<(), BoxError> {
    if pretty {
        writer.write_all(if index == 0 { b"\n" } else { b",\n" })?;
        let element = serde_json::to_string_pretty(item)?;
        for (line_index, line) in element.lines().enumerate() {
            if line_index > 0 {
                writer.write_all(b"\n")?;
            }
            // Elements sit two levels deep (indent 4) inside the root object.
            writer.write_all(b"    ")?;
            writer.write_all(line.as_bytes())?;
        }
    } else {
        if index > 0 {
            writer.write_all(b",")?;
        }
        serde_json::to_writer(&mut *writer, item)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_item_collection;
    use futures::stream;
    use serde_json::Value;
    use stac::{Item, Link, api::ItemCollection};

    /// `n` serialized STAC items and the same items as api items, so the stream
    /// input and the expected collection agree byte-for-byte.
    fn items(n: usize) -> (Vec<Value>, Vec<stac::api::Item>) {
        let api: Vec<stac::api::Item> = (0..n)
            .map(|i| Item::new(format!("item-{i}")).try_into().unwrap())
            .collect();
        let values = api
            .iter()
            .map(|i| serde_json::to_value(i).unwrap())
            .collect();
        (values, api)
    }

    async fn run(
        values: Vec<Value>,
        links: Vec<Link>,
        matched: Option<u64>,
        pretty: bool,
    ) -> Vec<u8> {
        let footer_links = links;
        let mut buf = Vec::new();
        write_item_collection(
            &mut buf,
            stream::iter(values.into_iter().map(Ok)),
            pretty,
            |_first, _last, _count| async move {
                let mut collection = ItemCollection::new(Vec::<stac::api::Item>::new()).unwrap();
                collection.links = footer_links;
                collection.number_matched = matched;
                Ok(collection)
            },
        )
        .await
        .unwrap();
        buf
    }

    #[tokio::test]
    async fn byte_identical_to_buffered() {
        let links = vec![Link::new("http://example.com/next?token=abc", "next")];
        for n in [0usize, 1, 2, 5] {
            for pretty in [false, true] {
                let matched = Some(n as u64 + 100);
                let (values, api) = items(n);
                let got = run(values, links.clone(), matched, pretty).await;

                let mut want_ic = ItemCollection::new(api).unwrap();
                want_ic.links = links.clone();
                want_ic.number_matched = matched;
                let want = if pretty {
                    serde_json::to_vec_pretty(&want_ic).unwrap()
                } else {
                    serde_json::to_vec(&want_ic).unwrap()
                };
                assert_eq!(
                    String::from_utf8(got).unwrap(),
                    String::from_utf8(want).unwrap(),
                    "n={n} pretty={pretty}"
                );
            }
        }
    }
}
