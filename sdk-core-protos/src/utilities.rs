pub trait TryIntoOrNone<F, T> {
    /// Turn an option of something into an option of another thing, trying to convert along the way
    /// and returning `None` if that conversion fails
    fn try_into_or_none(self) -> Option<T>;
}

impl<F, T> TryIntoOrNone<F, T> for Option<F>
where
    F: TryInto<T>,
{
    fn try_into_or_none(self) -> Option<T> {
        self.map(TryInto::try_into).transpose().ok().flatten()
    }
}
