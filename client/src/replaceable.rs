use crate::NamespacedClient;
use std::borrow::Cow;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

/// A client wrapper that allows replacing the underlying client at a later point in time.
/// Clones of [SharedReplaceableClient] have a shared reference to the underlying client, and also
/// a local cached clone of the underlying client. Before every service call, the cached clone is
/// updated if the shared client was replaced.
#[derive(Debug, Clone)]
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
    fn fetch_newer_than(&self, current_generation: u32) -> Option<(C, u32)> {
        (current_generation != self.generation.load(Ordering::Acquire)).then(|| {
            let lock = self.client.read().unwrap();
            let client = lock.clone();
            // Loading generation under lock to ensure the client won't be updated in the meantime.
            let generation = self.generation.load(Ordering::Acquire);
            (client, generation)
        })
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
    /// Use `clone` method to create more instances that share the same underlying client.
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

    /// Returns a reference to the underlying client if possible, or its clone otherwise.
    pub fn inner_cow(&self) -> Cow<'_, C> {
        self.shared_data
            .fetch_newer_than(self.cloned_generation)
            .map(|(c, _)| Cow::Owned(c))
            .unwrap_or_else(|| Cow::Borrowed(&self.cloned_client))
    }

    /// Refreshes this instance's cached clone of the underlying client. Returns a mutable reference
    /// to it. Called automatically by other mutable methods, in particular by all client calls.
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
