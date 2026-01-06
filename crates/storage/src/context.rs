//! Listing context wrappers for managing lifetime requirements.

use crate::error::StorageResult;
use crate::traits::{ListingOptions, ListingPage, ListingResume, ObjectStore};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;

/// A borrowed listing context with a lifetime tied to the ObjectStore.
///
/// This type wraps a reference to an ObjectStore and provides methods for creating
/// streams of object keys or pages. The streams have a lifetime tied to the context
/// (and thus the underlying store).
///
/// Use this type for non-spawned tasks where you have a direct borrow of the ObjectStore.
/// For spawned tasks that need 'static lifetimes, use ListingContextOwned instead.
///
/// # Lifetime
///
/// The 'a lifetime represents the borrow of the ObjectStore. All streams returned
/// from this context will have the same lifetime.
///
/// # Single-use pattern
///
/// This context is designed for single-use consumption. Once you call stream() or
/// stream_pages(), the context is consumed and cannot be reused.
pub struct ListingContext<'a> {
    store: &'a dyn ObjectStore,
    prefix: String,
    options: ListingOptions,
    resume: Option<ListingResume>,
}

impl<'a> ListingContext<'a> {
    /// Create a new listing context.
    ///
    /// # Arguments
    ///
    /// * `store` - Reference to the object store
    /// * `prefix` - Only list objects with this prefix
    /// * `options` - Page size and other listing options
    /// * `resume` - Optional continuation token to resume from
    pub fn new(
        store: &'a dyn ObjectStore,
        prefix: impl Into<String>,
        options: ListingOptions,
        resume: Option<ListingResume>,
    ) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            options,
            resume,
        }
    }

    /// Create a stream of object keys.
    ///
    /// This method consumes the context and returns a stream of keys.
    /// The stream has a lifetime tied to the ObjectStore borrow.
    ///
    /// # Returns
    ///
    /// A stream of object keys. Keys are yielded in the order they appear in the
    /// backend's listing (which may not be lexicographic).
    pub fn stream(self) -> Pin<Box<dyn Stream<Item = StorageResult<String>> + Send + 'a>> {
        let mut page_stream = self
            .store
            .list_pages(&self.prefix, self.options, self.resume);

        let stream = async_stream::try_stream! {
            while let Some(page_result) = page_stream.next().await {
                let page = page_result?;
                for key in page.keys {
                    yield key;
                }
            }
        };

        Box::pin(stream)
    }

    /// Create a stream of listing pages.
    ///
    /// This method consumes the context and returns a stream of pages, where each
    /// page contains a batch of keys and an optional continuation token.
    ///
    /// Use this when you need explicit control over pagination and checkpointing.
    ///
    /// # Returns
    ///
    /// A stream of ListingPage results. Each page contains up to page_size keys
    /// and an optional continuation token for the next page.
    pub fn stream_pages(
        self,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<ListingPage>> + Send + 'a>> {
        self.store
            .list_pages(&self.prefix, self.options, self.resume)
    }

    /// Get the prefix for this listing.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Get the options for this listing.
    pub fn options(&self) -> &ListingOptions {
        &self.options
    }
}

/// An owned listing context for spawned tasks with 'static lifetime requirements.
///
/// This type wraps an Arc<dyn ObjectStore> and provides methods for creating
/// streams with 'static lifetimes. Use this when you need to spawn tasks that
/// outlive the current scope.
///
/// # Lifetime
///
/// The streams returned by into_stream() and into_stream_pages() have 'static
/// lifetimes because they own an Arc to the ObjectStore.
///
/// # Borrowing
///
/// You can also create a borrowed ListingContext from this owned context using
/// borrow(). This allows you to use the context in non-spawned code while still
/// having the option to spawn tasks later.
pub struct ListingContextOwned {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    options: ListingOptions,
    resume: Option<ListingResume>,
}

impl ListingContextOwned {
    /// Create a new owned listing context.
    ///
    /// # Arguments
    ///
    /// * `store` - Arc to the object store
    /// * `prefix` - Only list objects with this prefix
    /// * `options` - Page size and other listing options
    /// * `resume` - Optional continuation token to resume from
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        options: ListingOptions,
        resume: Option<ListingResume>,
    ) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            options,
            resume,
        }
    }

    /// Borrow this owned context as a borrowed context.
    ///
    /// This allows you to use the context in non-spawned code while keeping
    /// the owned context available for later use.
    ///
    /// The returned context has a lifetime tied to the borrow of self.
    pub fn borrow(&self) -> ListingContext<'_> {
        ListingContext {
            store: self.store.as_ref(),
            prefix: self.prefix.clone(),
            options: self.options.clone(),
            resume: self.resume.clone(),
        }
    }

    /// Create a stream of object keys with 'static lifetime.
    ///
    /// This method consumes the context and returns a stream of keys that can
    /// be sent to spawned tasks.
    ///
    /// # Returns
    ///
    /// A stream of object keys with 'static lifetime.
    pub fn into_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<String>> + Send + 'static>> {
        let store = self.store.clone();
        let prefix = self.prefix;
        let options = self.options;
        let resume = self.resume;

        // Own the Arc inside the stream to keep the backend alive for its lifetime.
        let stream = async_stream::try_stream! {
            let mut page_stream = store.list_pages(&prefix, options, resume);
            while let Some(page_result) = page_stream.next().await {
                let page = page_result?;
                for key in page.keys {
                    yield key;
                }
            }
        };

        Box::pin(stream)
    }

    /// Create a stream of listing pages with 'static lifetime.
    ///
    /// This method consumes the context and returns a stream of pages that can
    /// be sent to spawned tasks.
    ///
    /// Use this when you need explicit control over pagination and checkpointing
    /// in spawned tasks.
    ///
    /// # Returns
    ///
    /// A stream of ListingPage results with 'static lifetime.
    pub fn into_stream_pages(
        self,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<ListingPage>> + Send + 'static>> {
        let store = self.store.clone();
        let prefix = self.prefix;
        let options = self.options;
        let resume = self.resume;

        // Own the Arc inside the stream to keep the backend alive for its lifetime.
        let stream = async_stream::try_stream! {
            let mut page_stream = store.list_pages(&prefix, options, resume);
            while let Some(page_result) = page_stream.next().await {
                yield page_result?;
            }
        };

        Box::pin(stream)
    }

    /// Get the prefix for this listing.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Get the options for this listing.
    pub fn options(&self) -> &ListingOptions {
        &self.options
    }

    /// Get the store.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
}

impl Clone for ListingContextOwned {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            prefix: self.prefix.clone(),
            options: self.options.clone(),
            resume: self.resume.clone(),
        }
    }
}
