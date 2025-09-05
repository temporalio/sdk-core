use crate::NamespacedClient;
use std::borrow::Cow;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

/// A client wrapper that allows replacing the underlying client at a later point in time.
/// Clones of this struct have a shared reference to the underlying client, and each clone also
/// has its own cached clone of the underlying client. Before every service call, a check is made
/// whether the shared client was replaced, and the cached clone is updated accordingly.
///
/// This struct is fully thread-safe, and it works in a lock-free manner except when the client is
/// being replaced. A read-write lock is used then, with minimal locking time.
#[derive(Debug)]
pub struct SharedReplaceableClient<C>
where
    C: Clone + Send + Sync,
{
    shared_data: Arc<SharedClientData<C>>,
    cloned_client: C,
    cloned_generation: u32,
}

#[derive(Debug)]
struct SharedClientData<C>
where
    C: Clone + Send + Sync,
{
    client: RwLock<C>,
    generation: AtomicU32,
}

impl<C> SharedClientData<C>
where
    C: Clone + Send + Sync,
{
    fn fetch(&self) -> (C, u32) {
        let lock = self.client.read().unwrap();
        let client = lock.clone();
        // Loading generation under lock to ensure the client won't be updated in the meantime.
        let generation = self.generation.load(Ordering::Acquire);
        (client, generation)
    }

    fn fetch_newer_than(&self, current_generation: u32) -> Option<(C, u32)> {
        // fetch() will do a second atomic load, but it's necessary to avoid a race condition.
        (current_generation != self.generation.load(Ordering::Acquire)).then(|| self.fetch())
    }

    fn replace_client(&self, client: C) {
        let mut lock = self.client.write().unwrap();
        *lock = client;
        // Updating generation under lock to guarantee consistency when multiple threads replace the
        // client at the same time. The client stored last is always the one with latest generation.
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
}

impl<C> SharedReplaceableClient<C>
where
    C: Clone + Send + Sync,
{
    /// Creates the initial instance of replaceable client with the provided underlying client.
    /// Use [`clone()`](Self::clone) method to create more instances that share the same underlying client.
    pub fn new(client: C) -> Self {
        let cloned_client = client.clone();
        Self {
            shared_data: Arc::new(SharedClientData {
                client: RwLock::new(client),
                generation: AtomicU32::new(0),
            }),
            cloned_client,
            cloned_generation: 0,
        }
    }

    /// Replaces the client for all instances that share this instance's underlying client.
    pub fn replace_client(&self, new_client: C) {
        self.shared_data.replace_client(new_client); // cloned_client will be updated on next mutable call
    }

    /// Returns a clone of the underlying client.
    pub fn inner_clone(&self) -> C {
        self.inner_cow().into_owned()
    }

    /// Returns a reference to this instance's cached clone of the underlying client if it's up to
    /// date, or a fresh clone of the shared client otherwise. Because it's an immutable method,
    /// it will not update this instance's cached clone. For this reason, prefer to use
    /// [`refresh_inner()`](Self::refresh_inner) when possible.
    pub fn inner_cow(&self) -> Cow<'_, C> {
        self.shared_data
            .fetch_newer_than(self.cloned_generation)
            .map(|(c, _)| Cow::Owned(c))
            .unwrap_or_else(|| Cow::Borrowed(&self.cloned_client))
    }

    /// Refreshes this instance's cached clone of the underlying client. Returns a mutable reference
    /// to it. Called automatically by other mutable methods, in particular by all RPC calls.
    ///
    /// While this method allows mutable access to the underlying client, any configuration changes
    /// will not be shared with other instances, and will be lost if the client gets replaced from
    /// anywhere. To make configuration changes, use [`replace_client()`](Self::refresh_client) instead.
    pub fn refresh_inner(&mut self) -> &mut C {
        if let Some((client, generation)) =
            self.shared_data.fetch_newer_than(self.cloned_generation)
        {
            self.cloned_client = client;
            self.cloned_generation = generation;
        }
        &mut self.cloned_client
    }
}

impl<C> Clone for SharedReplaceableClient<C>
where
    C: Clone + Send + Sync,
{
    /// Creates a new instance of replaceable client that shares the underlying client with this
    /// instance. Replacing a client in either instance will replace it for both instances, and all
    /// other clones too.
    fn clone(&self) -> Self {
        // self's cloned_client could've been modified through a mutable reference,
        // so for consistent behavior, we need to fetch it from shared_data.
        let (client, generation) = self.shared_data.fetch();
        Self {
            shared_data: self.shared_data.clone(),
            cloned_client: client,
            cloned_generation: generation,
        }
    }
}

impl<C> NamespacedClient for SharedReplaceableClient<C>
where
    C: NamespacedClient + Clone + Send + Sync,
{
    fn namespace(&self) -> String {
        self.inner_cow().namespace()
    }

    fn identity(&self) -> String {
        self.inner_cow().identity()
    }
}

#[cfg(test)]
mod tests {
    use crate::{NamespacedClient, SharedReplaceableClient};
    use std::borrow::Cow;

    #[derive(Debug, Clone)]
    struct StubClient {
        identity: String,
    }

    impl StubClient {
        fn new(identity: &str) -> Self {
            Self {
                identity: identity.to_owned(),
            }
        }
    }

    impl NamespacedClient for StubClient {
        fn namespace(&self) -> String {
            "default".into()
        }

        fn identity(&self) -> String {
            self.identity.clone()
        }
    }

    #[test]
    fn cow_returns_reference_before_and_clone_after_refresh() {
        let mut client = SharedReplaceableClient::new(StubClient::new("1"));
        let Cow::Borrowed(inner) = client.inner_cow() else {
            panic!("expected borrowed inner");
        };
        assert_eq!(inner.identity, "1");

        client.replace_client(StubClient::new("2"));
        let Cow::Owned(inner) = client.inner_cow() else {
            panic!("expected owned inner");
        };
        assert_eq!(inner.identity, "2");

        assert_eq!(client.refresh_inner().identity, "2");
        let Cow::Borrowed(inner) = client.inner_cow() else {
            panic!("expected borrowed inner");
        };
        assert_eq!(inner.identity, "2");
    }

    #[test]
    fn client_replaced_in_clones() {
        let original1 = SharedReplaceableClient::new(StubClient::new("1"));
        let clone1 = original1.clone();
        assert_eq!(original1.identity(), "1");
        assert_eq!(clone1.identity(), "1");

        original1.replace_client(StubClient::new("2"));
        assert_eq!(original1.identity(), "2");
        assert_eq!(clone1.identity(), "2");

        let original2 = SharedReplaceableClient::new(StubClient::new("3"));
        let clone2 = original2.clone();
        assert_eq!(original2.identity(), "3");
        assert_eq!(clone2.identity(), "3");

        clone2.replace_client(StubClient::new("4"));
        assert_eq!(original2.identity(), "4");
        assert_eq!(clone2.identity(), "4");
        assert_eq!(original1.identity(), "2");
        assert_eq!(clone1.identity(), "2");
    }
}
