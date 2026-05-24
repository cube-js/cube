pub trait UniqueVector<T> {
    fn unique_insert(&mut self, item: T) -> usize;
}

impl<T: PartialEq> UniqueVector<T> for Vec<T> {
    fn unique_insert(&mut self, item: T) -> usize {
        self.iter().position(|x| x == &item).unwrap_or_else(|| {
            let i = self.len();
            self.push(item);
            i
        })
    }
}
