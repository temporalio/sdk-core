use tokio::sync::watch;

/// A handy little tool that wraps an iterator and alerts a watch channel once the underlying
/// iterator has emptied out.
pub(crate) struct SignalWhenDonerator<Iter> {
    inner: Iter,
    notify_sender: Option<watch::Sender<bool>>,
}

impl<Iter, I> SignalWhenDonerator<Iter>
where
    Iter: Iterator<Item = I>,
{
    pub fn new(inner: Iter) -> (SignalWhenDonerator<Iter>, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (
            Self {
                inner,
                notify_sender: Some(tx),
            },
            rx,
        )
    }
}

impl<Iter> Iterator for SignalWhenDonerator<Iter>
where
    Iter: Iterator,
{
    type Item = Iter::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.inner.next();
        if res.is_none() {
            if let Some(tx) = self.notify_sender.take() {
                let _ = tx.send(true);
            }
        }
        res
    }
}
